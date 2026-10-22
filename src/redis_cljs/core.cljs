(ns redis-cljs.core
  "The redis-cljs client: construction of Redis clients from a kebab-case
   configuration map or the REDIS_* environment, and their lifecycle
   (connect, commands, blocking pops, end). Pools live in redis-cljs.pool
   (a pooled client is the same handle type), pub/sub channels in
   redis-cljs.pubsub. The public surface is plain ClojureScript data: a
   command is a vector ([:set \"key\" \"value\"]), a reply is what the
   server sent (a string, a number, nil, a vector, a Buffer on request),
   asynchronous operations return promesa promises, and blocking pops
   arrive on core.async channels. No JS interop appears at a call site;
   the `redis` (node-redis) objects stay inside the single binding
   namespace.

   Conventions:
   - names ending in `!` perform I/O and return a promise (brpop! and
     blpop! return the channel the pops arrive on; stop-feed! and end!
     return a promise);
   - a client is an opaque handle (printing it shows host, port, database,
     name and status, never the password);
   - every public function checks its arguments before any I/O and throws
     (or rejects with) a :redis-cljs/argument-error naming :function,
     :argument, :expected and :received; a configuration is refused as a
     :redis-cljs/config-error before anything is sent, with its secrets
     redacted from the error;
   - every other rejection is an ex-info of the one taxonomy
     (redis-cljs.util/error-types): :redis-cljs/network-error when the
     connection fails or is gone, :redis-cljs/api-error when the server
     answers a command with an error reply; an ex-info thrown by caller
     code passes through untouched;
   - a MULTI / EXEC reply whose elements include error replies resolves (a
     partial success) with those errors as :redis-cljs/warnings metadata
     on the vector; nothing else resolves partially;
   - values are strings or Buffers in and out and are never reinterpreted:
     no JSON, no number parsing; an integer reply is a number because RESP
     types it so;
   - one connection does one thing at a time: a blocking pop holds the
     connection until it answers, and a subscribed connection serves pub/sub
     only; use a second connect! or a pool for the rest;
   - the library records the client's `error` events on the handle
     (last-error) so that a connection breaking while idle never throws an
     unhandled error, and closes every channel of the client (pub/sub,
     pop feeds) when the connection ends or upstream gives up reconnecting;
     it adds no retries and no reconnection of its own (:reconnect false
     by default; see redis-cljs.util/client-config-keys)."
  (:require [cljs.core.async :as async]
            [cljs.core.async.impl.protocols :as impl]
            [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.util :as util]))

(def api-version
  "The library's own version marker, as a string. It names the version of
   the redis-cljs public surface, not the version of the wrapped `redis`
   package."
  "0.1.0")

(def default-buffer-size
  "Items a blocking-pop channel (brpop!, blpop!) holds before the next pop
   waits for the consumer: the fixed-buffer size used when :buffer-size is
   absent. redis-cljs.pubsub has its own, sliding, default of the same
   size."
  1024)

;; ---------------------------------------------------------------------------
;; The handle
;; ---------------------------------------------------------------------------

(defrecord Client
  ;; js: the upstream node-redis client. state: atom {:last-error e (the
  ;; last `error` event, as a network-error), :pooled? true for a client
  ;; handed out by redis-cljs.pool/with-client!, :subscriptions (the live
  ;; pub/sub channels of redis-cljs.pubsub), :feeds {chan feed} (the live
  ;; pop feeds), :end-hooks [f ...] (run once when the connection ends),
  ;; :closing? true once end! was called, :ended? true once the connection
  ;; ended}. The status is read live off the upstream flags, never cached.
  [js state])

(defn client?
  "True for a client handle produced by connect! or handed out by
   redis-cljs.pool/with-client!; false for anything else."
  [x]
  (instance? Client x))

(defn status
  "The state of the connection, synchronously: :ready (connected and the
   handshake done, commands run), :connecting (connect! called, not ready:
   connecting, or reconnecting when :reconnect is set), :closed (never
   connected, ended by end!, or the connection broke and the client does
   not reconnect). Throws an argument-error for a non-client."
  [client]
  (util/check! 'redis-cljs.core/status :client client? "a client handle" client)
  (let [{:keys [open? ready?]} (b/client-flags (:js client))]
    (cond
      (not open?) :closed
      ready? :ready
      :else :connecting)))

(extend-protocol IPrintWithWriter
  Client
  (-pr-writer [c w _]
    (-write w (str "#redis-cljs/client"
                   (pr-str (assoc (b/client-info (:js c)) :status (status c)))))))

(defn check-client!
  "Throws an argument-error (:argument :client of `function`) unless `c` is
   a client handle. Returns `c`. Shared with the pool and pubsub
   namespaces."
  [function c]
  (util/check! function :client client? "a client handle" c))

(defn last-error
  "The last `error` event the client emitted, as a :redis-cljs/network-error
   ex-info (its :cause is the upstream error), or nil when none was emitted.
   A socket error while idle, a reconnection attempt failing, a PING of
   :ping-interval-ms rejected: upstream reports them through this event,
   which the library records here instead of letting Node throw. Throws an
   argument-error for a non-client."
  [client]
  (check-client! 'redis-cljs.core/last-error client)
  (:last-error @(:state client)))

(defn on-end!
  "Registers `f`, a function of no arguments, to run once when the
   client's connection ends: after end!, when the socket closed and the
   client does not reconnect, or when its reconnect strategy gave up.
   Returns nil. Used by redis-cljs.pubsub to close its channels and by the
   pop feeds; available to callers for the same purpose. A hook
   registered after the connection ended runs at once. Throws an
   argument-error for a non-client or a non-function."
  [client f]
  (check-client! 'redis-cljs.core/on-end! client)
  (util/check! 'redis-cljs.core/on-end! :f fn? "a function of no arguments" f)
  (if (:ended? @(:state client))
    (f)
    (swap! (:state client) update :end-hooks (fnil conj []) f))
  nil)

(defn- run-end-hooks!
  "Marks the client ended and runs the registered hooks once; a hook
   registered later runs at once."
  [client]
  (let [[before _] (swap-vals! (:state client) assoc :ended? true :end-hooks [])
        hooks (:end-hooks before)]
    (doseq [f hooks] (f))))

(defn- ->network-error
  [op e]
  (util/normalize-failure op b/error-info e))

(defn- ->command-failure
  [op cmd e]
  (util/normalize-failure op b/error-info e {:command cmd}))

(defn wrap-client
  "A client handle around an upstream client object: attaches the `error`
   listener that records last-error and the `end` and `terminated`
   listeners that run the on-end! hooks. `pooled?` marks a client lent by
   a pool. Used by connect! and by redis-cljs.pool, once per upstream
   client; callers never need it. Throws an argument-error for a
   non-object or a non-boolean."
  [js-client pooled?]
  (util/check! 'redis-cljs.core/wrap-client :js-client b/event-emitter? "an upstream client object (an event emitter)" js-client)
  (util/check! 'redis-cljs.core/wrap-client :pooled? boolean? "true or false" pooled?)
  (let [handle (->Client js-client (atom {:pooled? pooled? :end-hooks []}))
        ended (fn [& _] (run-end-hooks! handle))]
    (b/on js-client "error" (fn [e] (swap! (:state handle) assoc :last-error (->network-error :event e))))
    (b/on js-client "end" ended)
    (b/on js-client "terminated" ended)
    handle))

;; ---------------------------------------------------------------------------
;; Construction
;; ---------------------------------------------------------------------------

(defn from-env
  "The configuration map read from the process environment through
   redis-cljs.util/env-keys: REDIS_URL -> :url, REDIS_HOST -> :host,
   REDIS_PORT -> :port, REDIS_USERNAME -> :username, REDIS_PASSWORD ->
   :password, REDIS_DATABASE -> :database, REDIS_TLS -> :tls. Each value is
   unwrapped by redis-cljs.util/unwrap-env-value (one layer of copy-paste
   quotes stripped); REDIS_PORT and REDIS_DATABASE become integers and
   REDIS_TLS a boolean (1 / true / yes / on, 0 / false / no / off), a
   config-error for anything else; blank variables are absent. Returns a
   map, {} when nothing is set (connect! then refuses it: the library never
   connects to a default host). Merge explicit keys over it to override a
   variable: (merge (from-env) {:name \"worker\"}). The library reads no
   file: keep secrets in the environment of the process."
  []
  (util/env->config (b/env)))

(defn- make-client
  "The upstream client for a validated configuration; a URL upstream
   cannot parse is a config-error naming :url."
  [cfg]
  (try
    (b/make-client (b/config->js (util/config->upstream cfg)))
    (catch :default e
      (if (util/passthrough-error? e)
        (throw e)
        (throw (util/config-error 'redis-cljs.core/connect! :url
                                  "a URL node-redis accepts with the other keys: redis://[[username][:password]@][host][:port][/db], rediss:// when :tls is true"
                                  (:url (util/redacted cfg))))))))

(defn connect!
  "Opens a connection and resolves with a client handle once the server is
   ready for commands (the handshake AUTH / CLIENT SETNAME / SELECT /
   CLIENT SETINFO done). `config` is a map of
   redis-cljs.util/client-config-keys (:url :host :port :tls :tls-options
   :username :password :database :name :connect-timeout-ms :reconnect
   :socket-timeout-ms :ping-interval-ms :disable-offline-queue), or
   (from-env); one of :url or :host is required; an unknown key, a wrong
   value or a malformed :url is a config-error before anything is sent
   (its :received redacted); a :url's own parts override the explicit
   keys, as in node-redis. Rejects with :redis-cljs/network-error when the
   socket fails (:code \"ECONNREFUSED\" ...), :connect-timeout-ms elapses
   (\"Connection timeout\") or the socket closes during the handshake, and
   with :redis-cljs/api-error when the server refuses the handshake
   (\"WRONGPASS ...\", \"NOAUTH ...\", \"ERR DB index is out of range\":
   :code is the first word). With :reconnect false (the default) the first
   failure is final; with a strategy set, connect! keeps retrying as
   upstream does and rejects only when the strategy gives up. A client is
   single-use: once ended it cannot reconnect; call connect! again."
  [config]
  (util/run-op (partial ->network-error :connect)
               (fn []
                 (let [cfg (-> config util/connect-config util/normalize-config util/check-config!)
                       js-client (make-client cfg)
                       handle (wrap-client js-client false)]
                   (p/then (b/connect js-client) (fn [_] handle))))))

;; ---------------------------------------------------------------------------
;; Commands
;; ---------------------------------------------------------------------------

(defn- with-warnings
  "A reply vector whose elements include error replies (EXEC with a failed
   command), with those errors as :redis-cljs/warnings metadata; any other
   reply as it is."
  [reply]
  (if-let [warnings (util/warnings-of b/error-element? reply)]
    (with-meta reply {:redis-cljs/warnings (vec warnings)})
    reply))

(defn command!
  "Runs one command and resolves with its reply. `cmd` is a vector whose
   first element names the command and whose other elements are its
   arguments: [:set \"key\" \"value\"], [:set \"key\" \"value\" :ex 60],
   [:hset \"h\" \"field\" \"value\"], [:rpush \"queue\" \"item\"],
   [:expire \"key\" 60], [\"CLIENT\" \"ID\"] or [:client-id]. The name is
   upper-cased (a keyword's dashes become spaces); a string or Buffer
   argument is sent as it is, a keyword argument as its upper-cased name
   (a modifier), a number as its decimal text, a BigInt as its decimal
   text; nil, booleans, collections, functions and any other value are an
   argument-error with :index, so is a command that is not a vector or
   whose name is blank. `opts` is {:buffers? true} to receive every bulk
   string of the reply as a Buffer instead of a string (a non-boolean is
   an argument-error).

   The reply is what the server sent, by RESP type: a simple string as a
   string (\"OK\", \"PONG\"), a bulk string as a string (or a Buffer with
   :buffers?), an integer as a number, a null bulk or null array as nil,
   an array as a vector of these (HGETALL under RESP2 is a flat vector of
   field, value, field, value; redis-cljs.helpers/pairs->map shapes it).
   Nothing is reinterpreted: a value stored as \"42\" comes back as the
   string \"42\". An EXEC reply whose elements include error replies (one
   command of the block failed, the others ran) RESOLVES with the vector,
   each failed element a map {:error-reply? true :message :name ...} and
   the vector carrying the metadata {:redis-cljs/warnings [{:index i :code
   \"WRONGTYPE\" :message \"...\"} ...]}: read it with meta.

   Rejects with :redis-cljs/api-error when the server answers with an
   error reply (:message the server's text verbatim, :code its first word,
   :command the vector as sent; the connection stays usable) and with
   :redis-cljs/network-error when the connection is gone (\"The client is
   closed\" after end!, \"Socket closed unexpectedly\", a socket error).
   Commands are pipelined in order on the one connection; a blocking
   command (BRPOP, BLPOP, BLMOVE, XREAD BLOCK) holds every command behind
   it until it answers."
  ([client cmd] (command! client cmd nil))
  ([client cmd opts]
   (util/run-op (partial ->command-failure :command cmd)
                (fn []
                  (check-client! 'redis-cljs.core/command! client)
                  (util/check-command! cmd)
                  (util/check-command-options! opts)
                  (let [js-args (b/command->js (util/command->upstream 'redis-cljs.core/command! cmd b/wire-object?))
                        js-opts (b/command-options->js opts)]
                    (p/then (b/send-command (:js client) js-args js-opts)
                            (comp with-warnings b/reply->data)))))))

;; ---------------------------------------------------------------------------
;; Blocking pops as channels
;; ---------------------------------------------------------------------------

(def pop-options
  "Options of brpop! and blpop!, each with the shape of its value:
   :timeout-s (the BRPOP / BLPOP timeout in seconds, a non-negative number,
   0 to block without limit; default 0), :buffer-size (a positive integer,
   the items held before the next pop waits for the consumer; default
   default-buffer-size), :buffers? (true or false, :key and :value as
   Buffers)."
  {:timeout-s #(and (number? %) (not (neg? %)))
   :buffer-size #(and (integer? %) (pos? %))
   :buffers? boolean?})

(defn- feed-of
  "The feed state of pop channel `ch` on `client`, nil when `ch` is not a
   live feed of this client."
  [client ch]
  (get-in @(:state client) [:feeds ch]))

(defn- finish-feed!
  "Closes the channel of a feed and forgets it (nothing to forget for a
   channel never registered, the one pop of an ended client; closing a
   closed channel is a no-op). What the channel holds (its buffer, and a
   put still waiting for room) stays takeable until exhausted, so
   nothing popped is lost."
  [client ch]
  (async/close! ch)
  (swap! (:state client) update :feeds dissoc ch))

(defn- settle-feed!
  "Stops the feed of `ch`: no further pop is sent; resolves with nil once
   the pop in flight, if any, has answered and the channel is closed."
  [client ch feed]
  (swap! feed assoc :stopped? true)
  (p/then (or (:in-flight @feed) (p/create (fn [resolve _] (resolve nil))))
          (fn [_]
            (when (feed-of client ch) (finish-feed! client ch))
            nil)))

(defn- settle-feeds!
  "settle-feed! for every live feed of the client; resolves with nil once
   all are closed."
  [client]
  (p/then (p/all (mapv (fn [[ch feed]] (settle-feed! client ch feed)) (:feeds @(:state client))))
          (fn [_] nil)))

(defn- pop-feed!
  [function command client keys opts]
  (check-client! function client)
  (let [ks (util/keys-argument function keys)
        _ (util/check-options! function pop-options opts)
        {:keys [timeout-s buffer-size buffers?] :or {timeout-s 0 buffer-size default-buffer-size}} opts
        ch (async/chan (async/buffer buffer-size))
        cmd (conj (into [command] ks) timeout-s)
        cmd-opts (when buffers? {:buffers? true})
        ;; :stopped? no further pop is sent; :in-flight a promise resolved
        ;; once the pop in flight has answered and its item (or error) has
        ;; been handed to the channel, nil when nothing is in flight
        feed (atom {:stopped? false :in-flight nil})]
    (letfn [(step []
              (when-not (or (:stopped? @feed) (impl/closed? ch))
                (let [in-flight (p/create
                                 (fn [resolve _]
                                   (-> (command! client cmd cmd-opts)
                                       (p/then (fn [reply]
                                                 (if (nil? reply)
                                                   (when (feed-of client ch) (finish-feed! client ch))
                                                   (async/put! ch {:key (first reply) :value (second reply)}
                                                               (fn [accepted?] (when accepted? (step)))))
                                                 (resolve nil)))
                                       (p/catch (fn [e]
                                                  (async/put! ch {:error e})
                                                  (finish-feed! client ch)
                                                  (resolve nil))))))]
                  (swap! feed assoc :in-flight in-flight)
                  (p/then in-flight (fn [_] (swap! feed (fn [f] (if (identical? (:in-flight f) in-flight) (assoc f :in-flight nil) f))))))))]
      (if (:ended? @(:state client))
        ;; on a client that already ended the one pop rejects at once, the
        ;; error reaches the channel and the channel is closed; the feed is
        ;; never registered (no end is left to wait for)
        (step)
        (do (swap! (:state client) assoc-in [:feeds ch] feed)
            (when-not (:end-hook-for-feeds? @(:state client))
              (swap! (:state client) assoc :end-hook-for-feeds? true)
              (on-end! client (fn [] (settle-feeds! client))))
            (step))))
    ch))

(defn brpop!
  "A core.async channel fed by BRPOP on `keys` (a key name or a vector of
   names, checked in order as Redis does): every item popped arrives as
   {:key \"queue\" :value \"item\"} (both Buffers with :buffers? true), in
   the order the server handed them out. BRPOP is sent again once the
   item has been accepted by the channel's buffer (:buffer-size, default
   1024, a FIXED buffer: when it is full the next pop waits for the
   consumer to take an item, so a slow consumer leaves the items in Redis
   and nothing is dropped). The feed goes on until:
   - a pop times out (:timeout-s elapsed with nothing to pop; 0, the
     default, never times out): the channel is closed, a take yields nil;
   - stop-feed! is called: no further pop is sent, the pop in flight is
     waited for and its item, if any, handed to the channel, then the
     channel is closed; what the channel holds stays takeable until
     exhausted, a put still waiting for room included: drain until nil;
   - the client ends (end!, or the connection gone for good): the same as
     stop-feed!, run by the library;
   - a rejection: the error (a :redis-cljs/api-error for a WRONGTYPE
     reply, a :redis-cljs/network-error when the connection is gone)
     arrives as {:error e}, then the channel is closed.
   Closing the channel yourself (cljs.core.async/close!) stops the feed
   too, but the item of the pop in flight is then lost to the closed
   channel: use stop-feed!. Returned synchronously; an argument-error is
   thrown for a non-client, bad keys, a negative :timeout-s, a
   non-positive :buffer-size, a non-boolean :buffers? or an unknown
   option. The pop holds the connection: give a feed its own connect!
   client (or a pooled client inside with-client!, and stop the feed
   before the function resolves), and keep in mind that end! waits for
   the pop in flight to answer (use {:force? true} to cut it)."
  ([client keys] (brpop! client keys nil))
  ([client keys opts]
   (pop-feed! 'redis-cljs.core/brpop! :brpop client keys opts)))

(defn blpop!
  "brpop! with BLPOP: the same channel, options and rules, popping from
   the head of the list instead of its tail (RPUSH + BLPOP is a FIFO
   queue)."
  ([client keys] (blpop! client keys nil))
  ([client keys opts]
   (pop-feed! 'redis-cljs.core/blpop! :blpop client keys opts)))

(defn stop-feed!
  "Stops the pop feed of channel `ch` (a channel brpop! or blpop! returned
   for `client`): no further pop is sent; the pop in flight, if any, is
   waited for (up to its :timeout-s) and its item handed to the channel;
   then the channel is closed. Resolves with nil once the channel is
   closed. What the channel holds stays takeable until exhausted, a put
   still waiting for room included (a full buffer never makes stop-feed!
   wait): drain until nil afterwards, nothing popped is lost. A channel
   whose feed already finished (timed out, errored, stopped, closed)
   resolves nil at once. Rejects with an argument-error for a non-client
   or a channel that is neither a feed of this client nor a closed
   channel. The wait can be long when the pop blocks without limit and
   the queue stays empty: give a feed a :timeout-s when it must stop
   promptly."
  [client ch]
  (util/run-op identity
               (fn []
                 (check-client! 'redis-cljs.core/stop-feed! client)
                 (if-let [feed (feed-of client ch)]
                   (settle-feed! client ch feed)
                   (if (and (satisfies? impl/Channel ch) (impl/closed? ch))
                     nil
                     (throw (util/argument-error 'redis-cljs.core/stop-feed! :ch
                                                 "a channel returned by brpop! or blpop! for this client, or a closed channel" ch)))))))

(defn feeds
  "The live pop channels of the client (feeds not yet finished), as a set
   (synchronous). Throws an argument-error for a non-client."
  [client]
  (check-client! 'redis-cljs.core/feeds client)
  (set (keys (:feeds @(:state client)))))

;; ---------------------------------------------------------------------------
;; Lifecycle
;; ---------------------------------------------------------------------------

(def end-options
  "Options of end!: :force? (true or false) destroys the socket at once
   instead of waiting for the pending commands."
  {:force? boolean?})

(defn end!
  "Closes the connection and resolves with nil. Upstream behavior, which
   redis-cljs keeps: the commands already sent are waited for (a blocking
   pop in flight included, up to its timeout), then the socket is closed;
   with {:force? true} the socket is destroyed at once, whether or not a
   graceful end! is still waiting, and every pending command rejects with
   a network-error (\"Disconnects client\"). The pop feeds of the client
   are stopped first (no further pop is sent; the one in flight hands its
   item to its channel) and end! resolves only once every feed channel is
   closed (what they hold stays takeable) and the on-end! hooks have run,
   which close the pub/sub channels. After end! every command! rejects
   with a network-error (\"The client is closed\"), status is :closed, and
   the client cannot be reconnected. A second graceful end! resolves when
   the first one does; a forced end! after a graceful one cuts the wait.
   A client lent by redis-cljs.pool/with-client! is not ended here but
   returned by the pool: an argument-error."
  ([client] (end! client nil))
  ([client opts]
   (util/run-op (partial ->network-error :end)
                (fn []
                  (check-client! 'redis-cljs.core/end! client)
                  (util/check-options! 'redis-cljs.core/end! end-options opts)
                  (util/check! 'redis-cljs.core/end! :client (complement (comp :pooled? deref :state))
                               "a client of connect! (a pooled client is returned by with-client!)" client)
                  (let [state (:state client)
                        settled (fn [] (settle-feeds! client))
                        finish (fn [_] (p/then (settled) (fn [_] (run-end-hooks! client) nil)))]
                    (doseq [[_ feed] (:feeds @state)]
                      (swap! feed assoc :stopped? true))
                    (cond
                      (:force? opts)
                      (if (:ended? @state)
                        (finish nil)
                        (do (swap! state assoc :closing? true)
                            (b/destroy (:js client))
                            (finish nil)))

                      (:closing? @state)
                      (if-let [closing (:closing @state)]
                        (p/then closing (fn [_] nil))
                        (finish nil))

                      (or (:ended? @state) (= :closed (status client)))
                      (finish nil)

                      :else
                      (let [closing (p/then (b/close (:js client)) finish)]
                        (swap! state assoc :closing? true :closing closing)
                        closing)))))))
