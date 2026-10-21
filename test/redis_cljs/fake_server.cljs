(ns redis-cljs.fake-server
  "A table-driven fake of a Redis SERVER for the offline suite and the
   examples: a real net.Server on a free port of 127.0.0.1 that speaks
   RESP2. The REAL node-redis client connects to it over TCP and runs its
   whole protocol (the handshake, pipelined commands, pub/sub, blocking
   commands); the fake parses the requests it receives (arrays of bulk
   strings, or inline commands), records every one, and answers from a
   script. It is test infrastructure, not a Redis: it stores nothing
   unless a script function of yours does.

   Script: {\"COMMAND\" spec, ...}, keyed by the upper-cased command name.
   A spec is a reply, or a function (fn [args ctx] -> reply) called with
   the request as a vector of strings (the command name first) and
   {:server s :socket sock}. A reply is:
   - nil                         -> a null bulk string ($-1)
   - a string or a Buffer        -> a bulk string
   - an integer                  -> an integer reply (:n)
   - a vector                    -> an array of these (*n)
   - (simple \"OK\")             -> a simple string (+OK)
   - (error \"WRONGTYPE ...\")   -> an error reply (-WRONGTYPE ...)
   - (delayed ms reply)          -> the reply, written ms later
   - (close-connection)          -> the socket is destroyed, no reply
   - (in-order r1 r2 ...)        -> a function spec: r1 to the first call,
                                    r2 to the second, the last one repeated.
   Unscripted commands have built-in answers that a script entry
   overrides: PING -> +PONG (or its argument), ECHO -> its argument,
   QUIT -> +OK then the socket ends, AUTH / SELECT / HELLO / CLIENT ...
   -> +OK, and the pub/sub trio: SUBSCRIBE / UNSUBSCRIBE answer their
   confirmations and PUBLISH pushes [\"message\" channel payload] to every
   connection subscribed to the channel and answers their count. Any
   other command is answered -ERR unscripted command 'NAME', so a test
   cannot pass on an accidental default.

   Per-test overrides: (reply-next! server \"HGET\" (error \"WRONGTYPE
   ...\")) answers the NEXT HGET with that reply, whatever the script
   says, then the script applies again.

   Every reply is written asynchronously (setImmediate, or after the
   delay), as a server would.

   Use: (p/let [server (start! {:script {\"GET\" \"value\"}})
                client (core/connect! (config server))] ...
          (commands server) ... (stop! server))"
  (:require [clojure.string :as str]
            [promesa.core :as p]))

(def ^:private net (js/require "net"))

;; ---------------------------------------------------------------------------
;; Reply specs
;; ---------------------------------------------------------------------------

(defn simple
  "A simple-string reply (+s)."
  [s]
  {::simple s})

(defn error
  "An error reply (-s), `s` the full text (\"WRONGTYPE Operation ...\")."
  [s]
  {::error s})

(defn delayed
  "`reply`, written `ms` milliseconds after the request arrived."
  [ms reply]
  {::delay-ms ms ::reply reply})

(defn close-connection
  "No reply: the connection is destroyed when this request arrives."
  []
  {::close? true})

(defn in-order
  "A function spec answering `replies` one per call, the last one
   repeated once they are exhausted."
  [& replies]
  (let [calls (atom 0)]
    (fn [_ _]
      (let [i (dec (swap! calls inc))]
        (nth replies (min i (dec (count replies))))))))

;; ---------------------------------------------------------------------------
;; RESP2 encoding
;; ---------------------------------------------------------------------------

(defn- bytes-of [s]
  (if (instance? js/Buffer s) s (js/Buffer.from (str s) "utf8")))

(defn- crlf [& parts]
  (js/Buffer.concat (to-array (conj (mapv bytes-of parts) (js/Buffer.from "\r\n")))))

(defn encode
  "A reply spec -> the RESP2 bytes (a Buffer)."
  [reply]
  (cond
    (nil? reply) (crlf "$-1")
    (map? reply) (cond
                   (contains? reply ::simple) (crlf "+" (::simple reply))
                   (contains? reply ::error) (crlf "-" (::error reply))
                   :else (throw (ex-info "fake-server: not a reply spec" {:reply reply})))
    (vector? reply) (js/Buffer.concat (to-array (into [(crlf "*" (count reply))] (map encode) reply)))
    (integer? reply) (crlf ":" reply)
    (or (string? reply) (instance? js/Buffer reply))
    (let [^js b (bytes-of reply)] (js/Buffer.concat #js [(crlf "$" (.-length b)) (crlf b)]))
    :else (throw (ex-info "fake-server: not a reply spec" {:reply reply}))))

;; ---------------------------------------------------------------------------
;; RESP2 request parsing: *N then N bulk strings, or an inline line
;; ---------------------------------------------------------------------------

(defn- line-end
  "Index of the \\r\\n at or after `from`, or -1."
  [^js buf from]
  (.indexOf buf "\r\n" from "utf8"))

(defn- parse-one
  "[request offset-after] for the request starting at `from` of `buf`, or
   nil when the bytes are not complete yet."
  [buf from]
  (let [eol (line-end ^js buf from)]
    (when-not (neg? eol)
      (let [line (.toString ^js buf "utf8" from eol)]
        (if (str/starts-with? line "*")
          (let [n (js/parseInt (subs line 1) 10)]
            (loop [i 0, off (+ eol 2), args []]
              (if (= i n)
                [args off]
                (let [eol (line-end buf off)]
                  (when-not (neg? eol)
                    (let [len (js/parseInt (.toString ^js buf "utf8" (inc off) eol) 10)
                          start (+ eol 2)
                          end (+ start len)]
                      (when (<= (+ end 2) (.-length ^js buf))
                        (recur (inc i) (+ end 2) (conj args (.toString ^js buf "utf8" start end))))))))))
          [(vec (remove str/blank? (str/split line #"\s+"))) (+ eol 2)])))))

(defn parse-requests
  "[requests rest]: every complete request at the head of `buf` (vectors
   of strings) and the bytes left over."
  [^js buf]
  (loop [off 0, requests []]
    (if-let [[request next-off] (and (< off (.-length buf)) (parse-one buf off))]
      (recur next-off (conj requests request))
      [requests (.subarray buf off)])))

;; ---------------------------------------------------------------------------
;; The server
;; ---------------------------------------------------------------------------

(defonce ^:private ports (atom #{}))

(defn live-ports
  "The ports of every fake server started and not yet stopped, as a set."
  []
  @ports)

(defonce ^:private released (atom #{}))

(defn released-ports
  "The ports closed-port handed out: ports of the loopback interface that
   were bound and released, so that nothing listens on them."
  []
  @released)

(defn closed-port
  "A promise of a port of 127.0.0.1 where nothing listens: a server is
   bound to a free port and closed again, and the port is returned. The
   refused-connection tests and examples use it instead of a fixed port
   that a machine might have in use."
  []
  (p/create
   (fn [resolve reject]
     (let [^js server (.createServer net)]
       (.once server "error" reject)
       (.listen server 0 "127.0.0.1"
                (fn []
                  (let [port (.-port (.address server))]
                    (.close server (fn [_]
                                     (swap! released conj port)
                                     (resolve port))))))))))

(defn- write-later!
  [^js socket delay-ms buf]
  (let [write (fn [] (when-not (.-destroyed ^js socket) (.write ^js socket buf)))]
    (if (pos? (or delay-ms 0))
      (js/setTimeout write delay-ms)
      (js/setImmediate write))))

(defn- handshake-command?
  "The CLIENT SETINFO pair node-redis sends on every connection; filtered
   out of `commands` so tests read what the library sent."
  [[name sub]]
  (and (= "CLIENT" (str/upper-case (str name))) (= "SETINFO" (str/upper-case (str sub)))))

(defn- subscriptions-of [state socket]
  (get-in state [:subscriptions socket] #{}))

(defn- pubsub-reply!
  "The built-in SUBSCRIBE / UNSUBSCRIBE / PUBLISH behavior: returns the
   reply (a vector of replies for the multi-channel confirmations)."
  [server socket name args]
  (let [state (:state server)]
    (case name
      "SUBSCRIBE"
      (mapv (fn [ch]
              (swap! state update-in [:subscriptions socket] (fnil conj #{}) ch)
              ["subscribe" ch (count (subscriptions-of @state socket))])
            args)

      "UNSUBSCRIBE"
      (let [chs (if (seq args) args (vec (subscriptions-of @state socket)))]
        (if (empty? chs)
          [["unsubscribe" nil 0]]
          (mapv (fn [ch]
                  (swap! state update-in [:subscriptions socket] (fnil disj #{}) ch)
                  ["unsubscribe" ch (count (subscriptions-of @state socket))])
                chs)))

      "PUBLISH"
      (let [[ch payload] args
            receivers (filter (fn [[_ chs]] (contains? chs ch)) (:subscriptions @state))]
        (doseq [[sock _] receivers]
          (write-later! sock 0 (encode ["message" ch payload])))
        (count receivers)))))

(defn- built-in-reply
  [server socket name args]
  (case name
    "PING" (simple (or (first args) "PONG"))
    "ECHO" (first args)
    "QUIT" (do (js/setImmediate (fn [] (.end socket))) (simple "OK"))
    ("AUTH" "SELECT" "HELLO" "CLIENT") (simple "OK")
    ("SUBSCRIBE" "UNSUBSCRIBE" "PUBLISH") (pubsub-reply! server socket name args)
    (error (str "ERR unscripted command '" name "'"))))

(defn- reply-for
  "The reply spec for a request: the one-shot override, the script entry,
   the built-in."
  [server socket request]
  (let [name (str/upper-case (str (first request)))
        args (vec (rest request))
        state (:state server)
        queue (get-in @state [:next name])
        override? (boolean (seq queue))
        _ (when override? (swap! state update-in [:next name] (comp vec rest)))
        spec (cond
               override? (first queue)
               (contains? (:script @state) name) (get (:script @state) name)
               :else ::built-in)
        spec (if (fn? spec) (spec request {:server server :socket socket}) spec)]
    (if (= ::built-in spec)
      (built-in-reply server socket name args)
      spec)))

(defn- answer!
  [server ^js socket request]
  (let [reply (reply-for server socket request)
        multi? (and (vector? reply) (contains? #{"SUBSCRIBE" "UNSUBSCRIBE"} (str/upper-case (str (first request)))))
        delay-ms (when (map? reply) (::delay-ms reply))
        reply (if (and (map? reply) (contains? reply ::reply)) (::reply reply) reply)]
    (cond
      (and (map? reply) (::close? reply)) (.destroy ^js socket)
      multi? (doseq [r reply] (write-later! socket delay-ms (encode r)))
      :else (write-later! socket delay-ms (encode reply)))))

(defn- on-connection!
  [server ^js socket]
  (let [state (:state server)
        pending (atom (js/Buffer.alloc 0))]
    (swap! state update :sockets conj socket)
    ;; A push written long after the last reply would otherwise wait behind
    ;; the delayed ACK of the peer (Nagle's algorithm): a server replies
    ;; promptly, so does the fake.
    (.setNoDelay socket true)
    (.on ^js socket "data"
         (fn [chunk]
           (let [[requests rest] (parse-requests (js/Buffer.concat #js [@pending chunk]))]
             (reset! pending rest)
             (doseq [request requests]
               (swap! state update :received conj request)
               (answer! server socket request)))))
    (.on ^js socket "error" (fn [_] nil))
    (.on ^js socket "close"
         (fn []
           (swap! state (fn [s] (-> s
                                    (update :sockets disj socket)
                                    (update-in [:subscriptions] dissoc socket)
                                    (update :closed inc))))))))

(defn start!
  "Starts a fake server on a free port of 127.0.0.1 and resolves with its
   handle: {:js the net.Server :port n :state atom}. `opts`: {:script
   {...}}."
  ([] (start! nil))
  ([{:keys [script]}]
   (p/create
    (fn [resolve reject]
      (let [state (atom {:script (into {} (map (fn [[k v]] [(str/upper-case (str k)) v])) script)
                         :next {}
                         :received []
                         :sockets #{}
                         :subscriptions {}
                         :closed 0})
            ^js js-server (.createServer net)
            server {:js js-server :state state}]
        (.on js-server "connection" (fn [socket] (on-connection! server socket)))
        (.once js-server "error" reject)
        (.listen js-server 0 "127.0.0.1"
                 (fn []
                   (let [port (.-port (.address js-server))]
                     (swap! ports conj port)
                     (resolve (assoc server :port port))))))))))

(defn stop!
  "Destroys every live connection of the server and closes it; resolves
   with nil."
  [server]
  (p/create
   (fn [resolve _]
     (doseq [^js socket (:sockets @(:state server))]
       (.destroy socket))
     (swap! ports disj (:port server))
     (.close ^js (:js server) (fn [_] (resolve nil))))))

;; ---------------------------------------------------------------------------
;; Reading and scripting
;; ---------------------------------------------------------------------------

(defn port
  "The port the server listens on."
  [server]
  (:port server))

(defn config
  "A connect! / pool! configuration pointing at the server (host/port
   form); `more` is merged over it."
  [server & [more]]
  (merge {:host "127.0.0.1" :port (:port server)} more))

(defn url
  "A redis:// URL pointing at the server, with `path` (\"/2\" for
   database 2) appended when given."
  [server & [path]]
  (str "redis://127.0.0.1:" (:port server) path))

(defn received
  "Every request the server received, from every connection, in order:
   a vector of vectors of strings (the command name as sent)."
  [server]
  (:received @(:state server)))

(defn commands
  "`received` without the CLIENT SETINFO pair node-redis sends on every
   connection, and with the command name upper-cased: what the library
   sent, as [\"SET\" \"k\" \"v\"]."
  [server]
  (into []
        (comp (remove handshake-command?)
              (map (fn [[name & args]] (into [(str/upper-case name)] args))))
        (received server)))

(defn reply-next!
  "Answers the next `command-name` request with `reply` (a spec), then
   the script applies again. Several calls queue up in order."
  [server command-name reply]
  (swap! (:state server) update-in [:next (str/upper-case (str command-name))] (fnil conj []) reply)
  nil)

(defn connections
  "The number of connections currently open."
  [server]
  (count (:sockets @(:state server))))

(defn closed-count
  "The number of connections that have closed since the server started."
  [server]
  (:closed @(:state server)))

(defn clear!
  "Forgets the requests received so far."
  [server]
  (swap! (:state server) assoc :received [])
  nil)
