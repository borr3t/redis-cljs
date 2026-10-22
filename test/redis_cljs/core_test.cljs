(ns redis-cljs.core-test
  "The client surface against the fake server (redis-cljs.fake-server):
   the REAL node-redis client speaks RESP2 to a scripted net.Server on a
   free port of 127.0.0.1, so connect!, command!, brpop! and end! run end
   to end with no Redis server and no network beyond the loopback
   interface. The one other socket opened goes to a port of 127.0.0.1
   that the fake bound and released, which refuses."
  (:require [cljs.test :refer [deftest is testing async]]
            [cljs.core.async :as async]
            [cljs.core.async.impl.protocols :as impl]
            [clojure.string :as str]
            [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as core]
            [redis-cljs.fake-server :as fake]
            [redis-cljs.util :as util]))

(deftest api-version-is-exact
  (is (= "0.1.0" core/api-version))
  (is (= "6.3.0" (b/redis-version)) "the pinned node-redis"))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- run
  "Runs the promise of (f) as an async test body; an unexpected rejection
   is a failed assertion."
  [done f]
  (-> (f)
      (p/catch (fn [e] (is false (str "unexpected rejection: " (ex-message e) " " (pr-str (ex-data e))))))
      (p/finally (fn [_ _] (done)))))

(defn- failure
  "Promise of the ex-data of the rejection of `pr` (nil when it resolved)."
  [pr]
  (-> pr
      (p/then (fn [v] (is false (str "expected a rejection, got " (pr-str v))) nil))
      (p/catch (fn [e] (ex-data e)))))

(defn- thrown-data [f]
  (try (f) nil (catch :default e (ex-data e))))

(defn- with-server
  "Starts a fake server with `script`, runs (f server) and stops the
   server whatever happened; the promise of what f resolved with."
  [script f]
  (p/let [server (fake/start! {:script script})]
    (p/let [outcome (-> (p/do (f server))
                        (p/then (fn [v] {:value v}))
                        (p/catch (fn [e] {:error e})))
            _ (fake/stop! server)]
      (if-let [e (:error outcome)]
        (throw e)
        (:value outcome)))))

(defn- take!
  "A promise of the next value of `ch`, or of ::timeout after `ms`."
  [ch & [ms]]
  (p/create (fn [resolve _]
              (async/take! ch resolve)
              (js/setTimeout #(resolve ::timeout) (or ms 1000)))))

(def ^:private round-trips
  "The scripted answers of the round-trip test: the command names and
   what the fake answers."
  {"SET" (fake/simple "OK")
   "SETNX" 1
   "MSET" (fake/simple "OK")
   "MGET" ["pencil" nil]
   "GET" (fake/in-order "pencil" nil)
   "INCR" 42
   "INCRBY" 47
   "DECR" 46
   "DECRBY" 40
   "APPEND" 11
   "STRLEN" 11
   "HSET" 2
   "HGET" "150"
   "HMGET" ["pencil" "150" nil]
   "HGETALL" ["name" "pencil" "price" "150"]
   "HDEL" 1
   "HEXISTS" 0
   "HINCRBY" 151
   "RPUSH" 2
   "LLEN" 2
   "LPUSH" 3
   "LPOP" "a"
   "RPOP" nil
   "LRANGE" ["a" "b"]
   "EXPIRE" 1
   "PEXPIRE" 1
   "TTL" (fake/in-order 60 -2)
   "PTTL" 59000
   "EXISTS" 1
   "DEL" 1
   "KEYS" ["product:1" "product:2"]
   "SCAN" ["0" ["product:1"]]
   "CLIENT" (fake/in-order (fake/simple "OK") (fake/simple "OK") 7)})

;; ---------------------------------------------------------------------------
;; Connecting
;; ---------------------------------------------------------------------------

(deftest connect-from-the-host-port-form
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [client (core/connect! (fake/config server {:username "worker" :password "s3cret"
                                                               :database 2 :name "tests"}))
                    status (core/status client)
                    ended (core/end! client)]
              (testing "the handle"
                (is (core/client? client))
                (is (not (core/client? {:js nil})))
                (is (= :ready status))
                (is (= :closed (core/status client)))
                (is (nil? (core/last-error client))))
              (testing "printing never shows the password"
                (let [shown (pr-str client)]
                  (is (str/starts-with? shown "#redis-cljs/client{"))
                  (is (str/includes? shown ":host \"127.0.0.1\""))
                  (is (str/includes? shown ":database 2"))
                  (is (str/includes? shown ":name \"tests\""))
                  (is (str/includes? shown ":status :closed"))
                  (is (not (str/includes? shown "s3cret")))))
              (testing "the handshake the real client sent, in order"
                (is (= [["AUTH" "worker" "s3cret"] ["CLIENT" "SETNAME" "tests"] ["SELECT" "2"]]
                       (fake/commands server)))
                (is (= 5 (count (fake/received server))) "plus the CLIENT SETINFO pair"))
              (testing "end! resolves nil and the socket closed"
                (is (nil? ended))
                (p/let [_ (p/delay 20)]
                  (is (= 1 (fake/closed-count server)))
                  (is (= 0 (fake/connections server))))))))))))

(deftest connect-from-the-url-form
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [client (core/connect! {:url (fake/url server "/3")})
                    pong (core/command! client [:ping])
                    _ (core/end! client)]
              (is (= [["SELECT" "3"] ["PING"]] (fake/commands server)))
              (is (= "PONG" pong))
              (testing "a URL inside a map, with the other keys"
                (p/let [client (core/connect! {:url (fake/url server) :name "by-url"})
                        _ (core/end! client)]
                  (is (= [["CLIENT" "SETNAME" "by-url"]] (subvec (fake/commands server) 2))))))))))))

(deftest a-bad-configuration-is-refused-before-any-socket
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [unknown (failure (core/connect! (fake/config server {:timeout 5})))
                    no-host (failure (core/connect! {:port (fake/port server)}))
                    wrong-port (failure (core/connect! {:host "127.0.0.1" :port "1"}))
                    not-a-config (failure (core/connect! 42))
                    from-nil (failure (core/connect! nil))
                    bare-url (failure (core/connect! (fake/url server)))
                    blank-host (failure (core/connect! {:host "" :port (fake/port server)}))
                    negative-db (failure (core/connect! (fake/config server {:database -1})))
                    bad-url (failure (core/connect! {:url "redis://127.0.0.1:notaport"}))
                    empty-host (failure (core/connect! {:url "redis:///0"}))
                    port-zero (failure (core/connect! {:url "redis://127.0.0.1:0/0"}))
                    negative-url-db (failure (core/connect! {:url "redis://127.0.0.1/-1"}))
                    fractional-db (failure (core/connect! {:url "redis://127.0.0.1/1.5"}))
                    tls-mismatch (failure (core/connect! {:url (fake/url server) :tls true}))
                    secret (failure (core/connect! (fake/config server {:password {:token "SYNTHETIC_SECRET"}})))
                    split-query (failure (core/connect! {:url "redis://u:SYNTHETIC?SECRET@127.0.0.1"}))
                    split-fragment (failure (core/connect! {:url "redis://u:SYNTHETIC#SECRET@127.0.0.1"}))]
              (is (= :redis-cljs/config-error (:type unknown)))
              (is (= #{:timeout} (:unknown unknown)))
              (is (= :redis-cljs/config-error (:type no-host)))
              (is (str/includes? (:expected no-host) ":url or :host"))
              (is (= :port (:argument wrong-port)))
              (is (= :redis-cljs/config-error (:type not-a-config)))
              (is (= :redis-cljs/config-error (:type from-nil)))
              (is (= :redis-cljs/config-error (:type bare-url)) "a bare URL string is not a configuration")
              (is (= :host (:argument blank-host)))
              (is (= :database (:argument negative-db)))
              (is (= :redis-cljs/config-error (:type bad-url)) "a URL node-redis cannot parse")
              (is (= {:function 'redis-cljs.core/connect! :argument :url} (select-keys bad-url [:function :argument])))
              (is (= :url (:argument empty-host)) "an empty host in a URL is refused")
              (is (= :port (:argument port-zero)))
              (is (= :url (:argument negative-url-db)))
              (is (= :url (:argument fractional-db)))
              (is (= :tls (:argument tls-mismatch)))
              (is (= "***" (:received secret)))
              (is (not (str/includes? (pr-str secret) "SYNTHETIC_SECRET")))
              (is (not (str/includes? (:message secret) "SYNTHETIC_SECRET")))
              (doseq [d [split-query split-fragment]]
                (is (= :url (:argument d)) "a raw ? or # in the password splits the URL: refused")
                (is (= "redis://***" (:received d)) "nothing after the scheme is carried")
                (is (not (str/includes? (pr-str d) "SYNTHETIC")))
                (is (not (str/includes? (:message d) "SYNTHETIC"))))
              (is (= [] (fake/received server)))
              (is (= 0 (fake/connections server)) "nothing was opened"))))))))

(deftest a-refused-connection-is-a-network-error
  ;; the port is one the fake just bound and released, so nothing listens
  ;; on it; this is the one socket of the suite that does not go to a fake
  ;; server
  (async done
    (run done
      (fn []
        (p/let [port (fake/closed-port)
                d (failure (core/connect! {:host "127.0.0.1" :port port :connect-timeout-ms 2000}))]
          (is (= :redis-cljs/network-error (:type d)))
          (is (= "ECONNREFUSED" (:code d)))
          (is (str/includes? (:message d) "ECONNREFUSED"))
          (is (= :connect (:op d)))
          (is (instance? js/Error (:cause d))))))))

(deftest a-refused-handshake-is-an-api-error
  (async done
    (run done
      (fn []
        (with-server {"AUTH" (fake/error "WRONGPASS invalid username-password pair or user is disabled.")}
          (fn [server]
            (p/let [d (failure (core/connect! (fake/config server {:password "nope"})))]
              (is (= :redis-cljs/api-error (:type d)))
              (is (= "WRONGPASS" (:code d)))
              (is (= "WRONGPASS invalid username-password pair or user is disabled." (:message d))))))))))

(deftest a-reconnect-strategy-that-gives-up-rejects
  (async done
    (run done
      (fn []
        (let [tries (atom 0)]
          (p/let [port (fake/closed-port)
                  d (failure (core/connect! {:host "127.0.0.1" :port port
                                             :reconnect (fn [retries _cause]
                                                          (swap! tries inc)
                                                          (if (< retries 2) 5 false))}))]
            (is (= :redis-cljs/network-error (:type d)))
            (is (= 3 @tries) "called at each failure until it answered false")))))))

(deftest from-env-reads-the-redis-variables
  ;; every REDIS_* variable of the process is cleared for the test and put
  ;; back afterwards, so an inherited one cannot change the outcome
  (let [names (filter #(str/starts-with? % "REDIS_") (js/Object.keys js/process.env))
        saved (into {} (map (fn [n] [n (unchecked-get js/process.env n)])) names)]
    (try
      (doseq [n names] (js-delete js/process.env n))
      (is (= {} (core/from-env)) "nothing set: an empty configuration")
      (set! (.-REDIS_HOST js/process.env) "'env-host'")
      (set! (.-REDIS_PORT js/process.env) "6380")
      (set! (.-REDIS_PASSWORD js/process.env) "\"p\"")
      (set! (.-REDIS_TLS js/process.env) "off")
      (is (= {:host "env-host" :port 6380 :password "p" :tls false} (core/from-env)))
      (set! (.-REDIS_TLS js/process.env) "flase")
      (is (= :tls (:argument (thrown-data #(core/from-env)))) "a typo fails loud")
      (finally
        (doseq [n (filter #(str/starts-with? % "REDIS_") (js/Object.keys js/process.env))]
          (js-delete js/process.env n))
        (doseq [[n v] saved] (unchecked-set js/process.env n v))))))

;; ---------------------------------------------------------------------------
;; Commands
;; ---------------------------------------------------------------------------

(deftest commands-round-trip-as-data
  (async done
    (run done
      (fn []
        (with-server round-trips
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    set (core/command! client [:set "product:1" "pencil" :ex 60])
                    setnx (core/command! client [:setnx "lock:1" "worker-1"])
                    mset (core/command! client [:mset "a" "1" "b" "2"])
                    mget (core/command! client [:mget "product:1" "product:9"])
                    get (core/command! client [:get "product:1"])
                    incr (core/command! client [:incr "counter"])
                    incrby (core/command! client [:incrby "counter" 5])
                    decr (core/command! client [:decr "counter"])
                    decrby (core/command! client [:decrby "counter" 6])
                    append (core/command! client [:append "product:1" " sharp"])
                    strlen (core/command! client [:strlen "product:1"])
                    hset (core/command! client [:hset "h:1" "name" "pencil" "price" 150])
                    hget (core/command! client [:hget "h:1" "price"])
                    hmget (core/command! client [:hmget "h:1" "name" "price" "missing"])
                    hgetall (core/command! client [:hgetall "h:1"])
                    hdel (core/command! client [:hdel "h:1" "price"])
                    hexists (core/command! client [:hexists "h:1" "price"])
                    hincrby (core/command! client [:hincrby "h:1" "price" 1])
                    rpush (core/command! client [:rpush "queue" "a" "b"])
                    llen (core/command! client [:llen "queue"])
                    lpush (core/command! client [:lpush "queue" "z"])
                    lpop (core/command! client [:lpop "queue"])
                    rpop (core/command! client [:rpop "empty"])
                    lrange (core/command! client [:lrange "queue" 0 -1])
                    expire (core/command! client [:expire "product:1" 60])
                    pexpire (core/command! client [:pexpire "product:1" 60000])
                    ttl (core/command! client [:ttl "product:1"])
                    pttl (core/command! client [:pttl "product:1"])
                    exists (core/command! client [:exists "product:1"])
                    del (core/command! client [:del "product:1"])
                    keys (core/command! client [:keys "product:*"])
                    scan (core/command! client [:scan 0 :match "product:*"])
                    client-id (core/command! client [:client-id])
                    _ (core/end! client)]
              (testing "replies are what the server sent: strings, numbers, nil, vectors"
                (is (= "OK" set))
                (is (= 1 setnx))
                (is (= "OK" mset))
                (is (= ["pencil" nil] mget))
                (is (= "pencil" get))
                (is (= 42 incr))
                (is (= 47 incrby))
                (is (= 46 decr))
                (is (= 40 decrby))
                (is (= 11 append))
                (is (= 11 strlen))
                (is (= 2 hset))
                (is (= "150" hget) "a number stored as text comes back as text")
                (is (= ["pencil" "150" nil] hmget))
                (is (= ["name" "pencil" "price" "150"] hgetall) "a flat vector under RESP2")
                (is (= 1 hdel))
                (is (= 0 hexists))
                (is (= 151 hincrby))
                (is (= 2 rpush))
                (is (= 2 llen))
                (is (= 3 lpush))
                (is (= "a" lpop))
                (is (nil? rpop))
                (is (= ["a" "b"] lrange))
                (is (= 1 expire))
                (is (= 1 pexpire))
                (is (= 60 ttl))
                (is (= 59000 pttl))
                (is (= 1 exists))
                (is (= 1 del))
                (is (= ["product:1" "product:2"] keys))
                (is (= ["0" ["product:1"]] scan))
                (is (= 7 client-id)))
              (testing "the commands the server received, verbatim"
                (is (= [["SET" "product:1" "pencil" "EX" "60"]
                        ["SETNX" "lock:1" "worker-1"]
                        ["MSET" "a" "1" "b" "2"]
                        ["MGET" "product:1" "product:9"]
                        ["GET" "product:1"]
                        ["INCR" "counter"]
                        ["INCRBY" "counter" "5"]
                        ["DECR" "counter"]
                        ["DECRBY" "counter" "6"]
                        ["APPEND" "product:1" " sharp"]
                        ["STRLEN" "product:1"]
                        ["HSET" "h:1" "name" "pencil" "price" "150"]
                        ["HGET" "h:1" "price"]
                        ["HMGET" "h:1" "name" "price" "missing"]
                        ["HGETALL" "h:1"]
                        ["HDEL" "h:1" "price"]
                        ["HEXISTS" "h:1" "price"]
                        ["HINCRBY" "h:1" "price" "1"]
                        ["RPUSH" "queue" "a" "b"]
                        ["LLEN" "queue"]
                        ["LPUSH" "queue" "z"]
                        ["LPOP" "queue"]
                        ["RPOP" "empty"]
                        ["LRANGE" "queue" "0" "-1"]
                        ["EXPIRE" "product:1" "60"]
                        ["PEXPIRE" "product:1" "60000"]
                        ["TTL" "product:1"]
                        ["PTTL" "product:1"]
                        ["EXISTS" "product:1"]
                        ["DEL" "product:1"]
                        ["KEYS" "product:*"]
                        ["SCAN" "0" "MATCH" "product:*"]
                        ["CLIENT" "ID"]]
                       (fake/commands server)))))))))))

(deftest buffers-in-and-out
  (async done
    (run done
      (fn []
        (with-server {"SET" (fake/simple "OK")
                      "GET" (js/Buffer.from #js [0 255 1])
                      "MGET" [(js/Buffer.from "a") nil]}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    _ (core/command! client [:set "bin" (js/Buffer.from #js [1 2 3])])
                    as-string (core/command! client [:get "bin"])
                    as-buffer (core/command! client [:get "bin"] {:buffers? true})
                    many (core/command! client [:mget "bin" "none"] {:buffers? true})
                    _ (core/end! client)]
              (is (string? as-string) "without the option, a bulk string is decoded as UTF-8")
              (is (b/buffer? as-buffer))
              (is (= [0 255 1] (vec (js/Array.from as-buffer))))
              (is (b/buffer? (first many)))
              (is (= "a" (.toString (first many))))
              (is (nil? (second many)))
              (is (= [["SET" "bin" (str (js/Buffer.from #js [1 2 3]))]]
                     (subvec (fake/commands server) 0 1)) "the Buffer argument went as its bytes"))))))))

(deftest an-error-reply-is-an-api-error-with-the-message-verbatim
  (async done
    (run done
      (fn []
        (with-server {"GET" "v" "HGET" "x"}
          (fn [server]
            (fake/reply-next! server "HGET" (fake/error "WRONGTYPE Operation against a key holding the wrong kind of value"))
            (p/let [client (core/connect! (fake/config server))
                    d (failure (core/command! client [:hget "s" "f"]))
                    again (core/command! client [:hget "s" "f"])
                    unknown (failure (core/command! client [:frobnicate "x"]))
                    still (core/command! client [:get "k"])
                    _ (core/end! client)]
              (is (= :redis-cljs/api-error (:type d)))
              (is (= "WRONGTYPE Operation against a key holding the wrong kind of value" (:message d)))
              (is (= "WRONGTYPE" (:code d)))
              (is (= [:hget "s" "f"] (:command d)))
              (is (= :command (:op d)))
              (is (= "x" again) "the next HGET follows the script again")
              (is (= "ERR" (:code unknown)))
              (is (= "ERR unscripted command 'FROBNICATE'" (:message unknown)))
              (is (= "v" still) "the connection stays usable"))))))))

(deftest a-mixed-exec-reply-resolves-with-warnings
  (async done
    (run done
      (fn []
        (with-server {"MULTI" (fake/simple "OK")
                      "SET" (fake/simple "QUEUED")
                      "HGET" (fake/simple "QUEUED")
                      "EXEC" [(fake/simple "OK")
                              (fake/error "WRONGTYPE Operation against a key holding the wrong kind of value")]}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    _ (core/command! client [:multi])
                    _ (core/command! client [:set "k" "v"])
                    _ (core/command! client [:hget "k" "f"])
                    r (core/command! client [:exec])
                    _ (core/end! client)]
              (is (= "OK" (first r)))
              (is (= {:error-reply? true :message "WRONGTYPE Operation against a key holding the wrong kind of value" :name "SimpleError"}
                     (second r)))
              (is (= [{:index 1 :code "WRONGTYPE" :message "WRONGTYPE Operation against a key holding the wrong kind of value"}]
                     (:redis-cljs/warnings (meta r)))))))))))

(deftest option-values-are-checked-not-only-their-names
  (async done
    (run done
      (fn []
        (with-server {"GET" "v"}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    rejected (fn [pr] (p/catch pr ex-data))
                    buffers (rejected (core/command! client [:get "k"] {:buffers? "false"}))
                    force (rejected (core/end! client {:force? "false"}))
                    still (core/command! client [:get "k"])
                    _ (core/end! client)]
              (is (= {:argument :buffers? :expected "true or false" :received "false"}
                     (select-keys buffers [:argument :expected :received])))
              (is (= {:argument :force? :expected "true or false" :received "false"}
                     (select-keys force [:argument :expected :received])))
              (is (= "v" still) "the refused end! ended nothing")
              (is (= :buffers? (:argument (thrown-data #(core/brpop! client "q" {:buffers? 1}))))))))))))

(deftest expiry-as-the-server-reports-it
  (async done
    (run done
      (fn []
        (with-server {"SET" (fake/simple "OK")
                      "EXPIRE" 1
                      "TTL" (fake/in-order 30 -2)
                      "GET" (fake/in-order "pencil" nil)}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    _ (core/command! client [:set "k" "pencil"])
                    _ (core/command! client [:expire "k" 30])
                    before [(core/command! client [:ttl "k"]) (core/command! client [:get "k"])]
                    before (p/all before)
                    after (p/all [(core/command! client [:ttl "k"]) (core/command! client [:get "k"])])
                    _ (core/end! client)]
              (is (= [30 "pencil"] before))
              (is (= [-2 nil] after) "once the key expired: TTL -2, GET nil"))))))))

(deftest a-delayed-reply-is-waited-for
  (async done
    (run done
      (fn []
        (with-server {"GET" (fake/delayed 60 "late")}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    t0 (js/Date.now)
                    v (core/command! client [:get "k"])
                    _ (core/end! client)]
              (is (= "late" v))
              (is (>= (- (js/Date.now) t0) 50)))))))))

;; ---------------------------------------------------------------------------
;; Blocking pops
;; ---------------------------------------------------------------------------

(deftest brpop-feeds-a-channel-until-the-timeout
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/in-order ["jobs" "1"] (fake/delayed 30 ["jobs" "2"]) ["jobs" "3"] (fake/delayed 20 nil))}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/brpop! client "jobs" {:timeout-s 1})
                    a (take! ch)
                    b (take! ch)
                    c (take! ch)
                    end (take! ch)
                    _ (core/end! client)]
              (is (= {:key "jobs" :value "1"} a))
              (is (= {:key "jobs" :value "2"} b))
              (is (= {:key "jobs" :value "3"} c))
              (is (nil? end) "the timeout closes the channel: nil, no hang")
              (is (impl/closed? ch))
              (is (= [["BRPOP" "jobs" "1"] ["BRPOP" "jobs" "1"] ["BRPOP" "jobs" "1"] ["BRPOP" "jobs" "1"]]
                     (fake/commands server)) "one BRPOP per item, then the one that timed out"))))))))

(deftest pops-with-buffers-give-buffers-for-key-and-value
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/in-order ["q" (js/Buffer.from #js [0 255])] nil)}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/brpop! client "q" {:buffers? true :timeout-s 1})
                    m (take! ch)
                    _ (take! ch)
                    _ (core/end! client)]
              (is (b/buffer? (:key m)))
              (is (b/buffer? (:value m)))
              (is (= "q" (.toString (:key m))))
              (is (= [0 255] (vec (js/Array.from (:value m))))))))))))

(deftest blpop-on-several-keys
  (async done
    (run done
      (fn []
        (with-server {"BLPOP" (fake/in-order ["high" "h1"] nil)}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/blpop! client ["high" "low"])
                    a (take! ch)
                    end (take! ch)
                    _ (core/end! client)]
              (is (= {:key "high" :value "h1"} a))
              (is (nil? end))
              (is (= [["BLPOP" "high" "low" "0"] ["BLPOP" "high" "low" "0"]] (fake/commands server))))))))))

(deftest stop-feed-waits-for-the-pop-in-flight-and-loses-nothing
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/in-order ["q" "1"] (fake/delayed 60 ["q" "2"]) ["q" "3"])}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/brpop! client "q")
                    first-item (take! ch)
                    live (core/feeds client)
                    ;; the second pop is in flight (its reply comes 60 ms later)
                    _ (core/stop-feed! client ch)
                    sent (count (fake/commands server))
                    rest (take-while some? (repeatedly #(async/poll! ch)))
                    again (core/stop-feed! client ch)
                    _ (core/end! client)]
              (is (= {:key "q" :value "1"} first-item))
              (is (= #{ch} live))
              (is (= [{:key "q" :value "2"}] rest) "the item of the pop in flight was delivered, then the channel closed")
              (is (impl/closed? ch))
              (is (= 2 sent) "no pop was sent after the stop")
              (is (= #{} (core/feeds client)))
              (is (nil? again) "stopping a finished feed resolves nil")
              (p/let [foreign (failure (core/stop-feed! client (async/chan)))
                      not-client (failure (core/stop-feed! 1 ch))]
                (is (= :ch (:argument foreign)))
                (is (= 'redis-cljs.core/stop-feed! (:function not-client)))
                (is (= :client (:argument not-client)))))))))))

(deftest stop-feed-never-waits-for-a-full-buffer
  ;; a buffer of one: one item buffered, the next put waiting for room;
  ;; stop-feed! awaited BEFORE draining resolves, and the drain yields
  ;; both items
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/in-order ["q" "1"] ["q" "2"] ["q" "3"])}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/brpop! client "q" {:buffer-size 1})
                    _ (p/delay 50)
                    sent (count (fake/commands server))
                    stopped (core/stop-feed! client ch)
                    drained (take-while some? (repeatedly #(async/poll! ch)))
                    _ (core/end! client)]
              (is (= 2 sent) "the second pop's put waits for room; no third pop")
              (is (nil? stopped))
              (is (= [{:key "q" :value "1"} {:key "q" :value "2"}] drained) "nothing popped is lost")
              (is (impl/closed? ch)))))))))

(deftest stop-feed-checks-its-channel-first
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    results (p/all (mapv (fn [x] (p/catch (core/stop-feed! client x) ex-data)) [nil 7 "x" (async/chan)]))
                    closed (core/stop-feed! client (doto (async/chan) async/close!))
                    _ (core/end! client)]
              (doseq [d results]
                (is (= :redis-cljs/argument-error (:type d)))
                (is (= :ch (:argument d)))
                (is (= 'redis-cljs.core/stop-feed! (:function d))))
              (is (nil? closed) "a closed channel is a finished feed"))))))))

(deftest a-graceful-end-stops-the-feeds-and-closes-them
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/in-order (fake/delayed 40 ["q" "1"]) ["q" "2"])}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/brpop! client "q")
                    _ (core/end! client)
                    _ (p/delay 20)
                    items (take-while some? (repeatedly #(async/poll! ch)))]
              (is (= [{:key "q" :value "1"}] items) "the pop in flight delivered its item; no further pop")
              (is (impl/closed? ch))
              (is (= 1 (count (fake/commands server))))
              (is (= #{} (core/feeds client))))))))))

(deftest end-resolves-once-the-feeds-are-closed
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/delayed 40 ["q" "1"])}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/brpop! client "q" {:buffer-size 1})
                    _ (core/end! client)]
              (is (impl/closed? ch) "closed by the time end! resolved")
              (is (= #{} (core/feeds client)))
              (is (= [{:key "q" :value "1"}] (take-while some? (repeatedly #(async/poll! ch))))))))))))

(deftest a-pop-error-arrives-on-the-channel
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/error "WRONGTYPE Operation against a key holding the wrong kind of value")}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    ch (core/brpop! client "notalist")
                    m (take! ch)
                    end (take! ch)
                    _ (core/end! client)]
              (is (= :redis-cljs/api-error (:type (ex-data (:error m)))))
              (is (= "WRONGTYPE" (:code (ex-data (:error m)))))
              (is (nil? end) "closed after the error"))))))))

(deftest a-pop-after-end-reports-the-closed-client
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    _ (core/end! client)
                    ch (core/brpop! client "q")
                    m (take! ch)
                    closed (take! ch)
                    stopped (core/stop-feed! client ch)]
              (is (= :redis-cljs/network-error (:type (ex-data (:error m)))))
              (is (= "The client is closed" (:message (ex-data (:error m)))))
              (is (nil? closed) "the channel closes after the error")
              (is (= #{} (core/feeds client)))
              (is (nil? stopped) "stop-feed! takes the closed channel"))))))))

(deftest trailing-slash-and-empty-port-urls-connect
  (async done
    (run done
      (fn []
        (with-server {"PING" (fake/simple "PONG")}
          (fn [server]
            (p/let [slash (core/connect! {:url (fake/url server "/")})
                    pong-1 (core/command! slash [:ping])
                    _ (core/end! slash)
                    ;; an empty port in the URL leaves the explicit :port in force
                    empty-port (core/connect! {:url "redis://127.0.0.1:" :port (fake/port server)})
                    pong-2 (core/command! empty-port [:ping])
                    _ (core/end! empty-port)]
              (is (= "PONG" pong-1))
              (is (= "PONG" pong-2))
              (is (= [["PING"] ["PING"]] (fake/commands server))))))))))

;; ---------------------------------------------------------------------------
;; Ending
;; ---------------------------------------------------------------------------

(deftest commands-after-end-reject
  (async done
    (run done
      (fn []
        (with-server {"GET" "v"}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    _ (core/end! client)
                    again (core/end! client)
                    d (failure (core/command! client [:get "k"]))]
              (is (nil? again) "a second end! resolves nil")
              (is (= :redis-cljs/network-error (:type d)))
              (is (= "The client is closed" (:message d)))
              (is (= "ClientClosedError" (:name d)))
              (is (= :closed (core/status client))))))))))

(deftest end-waits-for-the-pending-reply-unless-forced
  (async done
    (run done
      (fn []
        (with-server {"GET" (fake/delayed 80 "late")}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    [v waited] (let [pending (core/command! client [:get "k"])
                                     t0 (js/Date.now)]
                                 (p/let [_ (core/end! client)
                                         v pending]
                                   [v (- (js/Date.now) t0)]))
                    forced (core/connect! (fake/config server))
                    cut (let [pr (failure (core/command! forced [:get "k"]))]
                          (p/let [_ (core/end! forced {:force? true})]
                            pr))]
              (is (= "late" v))
              (is (>= waited 60) "end! waited for the reply")
              (is (= :redis-cljs/network-error (:type cut)))
              (is (= "Disconnects client" (:message cut)))
              (is (= :closed (core/status forced))))))))))

(deftest a-forced-end-cuts-a-graceful-end-still-waiting
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/delayed 5000 ["q" "never"])}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    [graceful cut] (let [pending (p/catch (core/command! client [:brpop "q" 0]) ex-data)
                                         graceful (core/end! client)]
                                     ;; the graceful end! waits for the pop; the forced one cuts it
                                     (p/let [_ (p/delay 30)
                                             _ (core/end! client {:force? true})
                                             g graceful
                                             c pending]
                                       [g c]))]
              (is (nil? graceful) "the graceful end! resolved once the socket was destroyed")
              (is (= :redis-cljs/network-error (:type cut)))
              (is (= "Disconnects client" (:message cut)))
              (is (= :closed (core/status client))))))))))

(deftest a-second-graceful-end-does-not-disable-the-forced-one
  (async done
    (run done
      (fn []
        (with-server {"BRPOP" (fake/delayed 5000 ["q" "never"])}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    [first-end second-end cut] (let [pending (p/catch (core/command! client [:brpop "q" 0]) ex-data)
                                                     first-end (core/end! client)
                                                     second-end (core/end! client)]
                                                 (p/let [_ (p/delay 30)
                                                         _ (core/end! client {:force? true})
                                                         a first-end
                                                         b second-end
                                                         c pending]
                                                   [a b c]))]
              (is (nil? first-end))
              (is (nil? second-end) "the second graceful end! resolved with the first")
              (is (= "Disconnects client" (:message cut)) "the forced end! destroyed the socket")
              (is (= :closed (core/status client))))))))))

(deftest a-broken-connection-is-recorded-and-closes-every-channel
  (async done
    (run done
      (fn []
        (with-server {"GET" (fake/close-connection) "BRPOP" (fake/delayed 5000 ["q" "never"])}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    feed (core/brpop! client "q")
                    d (failure (core/command! client [:get "k"]))
                    _ (p/delay 30)
                    feed-message (async/poll! feed)
                    ended (core/end! client)]
              (is (= :redis-cljs/network-error (:type d)))
              (is (= "Socket closed unexpectedly" (:message d)))
              (is (= :closed (core/status client)) "no reconnect by default")
              (is (= :redis-cljs/network-error (:type (ex-data (core/last-error client)))))
              (is (= "Socket closed unexpectedly" (:message (ex-data (core/last-error client)))))
              (is (= :redis-cljs/network-error (:type (ex-data (:error feed-message)))) "the pop in flight failed onto the feed")
              (is (impl/closed? feed) "the feed closed with the connection")
              (is (= #{} (core/feeds client)))
              (is (nil? ended) "end! after the break resolves nil"))))))))

(deftest a-reconnect-strategy-giving-up-later-closes-every-channel
  ;; upstream counts retries within one outage, so a strategy that gives
  ;; up on the second OUTAGE counts them itself: the first break is
  ;; followed by a reconnection (the fake accepts it), the second makes
  ;; the strategy answer false, upstream emits `terminated`, and the
  ;; library runs the end hooks then
  (async done
    (run done
      (fn []
        (with-server {"GET" (fake/close-connection)}
          (fn [server]
            (let [outages (atom 0)
                  hooks (atom 0)]
              (p/let [client (core/connect! (fake/config server {:reconnect (fn [_ _] (if (= 1 (swap! outages inc)) 10 false))}))
                      _ (core/on-end! client #(swap! hooks inc))
                      first-break (failure (core/command! client [:get "k"]))
                      _ (p/delay 80)
                      reconnected (core/status client)
                      hooks-meanwhile @hooks
                      second-break (failure (core/command! client [:get "k"]))
                      _ (p/delay 80)
                      status (core/status client)
                      _ (core/end! client)]
                (is (= "Socket closed unexpectedly" (:message first-break)))
                (is (= :ready reconnected) "the strategy reconnected once")
                (is (= 0 hooks-meanwhile) "no end hook while the client reconnects")
                (is (= "Socket closed unexpectedly" (:message second-break)))
                (is (= :closed status))
                (is (= 1 @hooks) "the end hooks ran once when the strategy gave up")
                (is (= 2 (fake/closed-count server)))))))))))

(deftest end-hooks-run-once-when-the-connection-ends
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (let [ran (atom 0)]
              (p/let [client (core/connect! (fake/config server))
                      _ (core/on-end! client #(swap! ran inc))
                      _ (core/end! client)
                      _ (p/delay 20)]
                (is (= 1 @ran))))))))))

;; ---------------------------------------------------------------------------
;; The argument contract
;; ---------------------------------------------------------------------------

(deftest every-function-checks-its-arguments
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    rejected (fn [pr] (p/catch pr ex-data))
                    not-client (rejected (core/command! {:js nil} [:get "k"]))
                    bad-command (rejected (core/command! client "GET k"))
                    bad-arg (rejected (core/command! client [:set "k" nil]))
                    fn-arg (rejected (core/command! client [:set "k" (fn [] "v")]))
                    blank-name (rejected (core/command! client ["" "k"]))
                    blank-keyword (rejected (core/command! client [(keyword "") "k"]))
                    dash-name (rejected (core/command! client [:- "k"]))
                    bad-opts (rejected (core/command! client [:get "k"] {:buffer true}))
                    end-not-client (rejected (core/end! 7))
                    end-opts (rejected (core/end! client {:now true}))
                    _ (core/end! client)]
              (doseq [[d function argument] [[not-client 'redis-cljs.core/command! :client]
                                              [bad-command 'redis-cljs.core/command! :command]
                                              [bad-arg 'redis-cljs.core/command! :command]
                                              [fn-arg 'redis-cljs.core/command! :command]
                                              [blank-name 'redis-cljs.core/command! :command]
                                              [blank-keyword 'redis-cljs.core/command! :command]
                                              [dash-name 'redis-cljs.core/command! :command]
                                              [bad-opts 'redis-cljs.core/command! :opts]
                                              [end-not-client 'redis-cljs.core/end! :client]
                                              [end-opts 'redis-cljs.core/end! :opts]]]
                (is (= :redis-cljs/argument-error (:type d)) (str function " " argument))
                (is (= function (:function d)))
                (is (= argument (:argument d)))
                (is (string? (:expected d)))
                (is (contains? d :received)))
              (is (= 2 (:index bad-arg)))
              (is (= 2 (:index fn-arg)))
              (is (= 0 (:index blank-name)))
              (is (not (str/includes? (:message bad-arg) "nil")) "a message names no value")
              (testing "the synchronous ones throw"
                (doseq [[f function argument] [[#(core/status 1) 'redis-cljs.core/status :client]
                                                [#(core/last-error 1) 'redis-cljs.core/last-error :client]
                                                [#(core/on-end! 1 identity) 'redis-cljs.core/on-end! :client]
                                                [#(core/on-end! client 1) 'redis-cljs.core/on-end! :f]
                                                [#(core/brpop! 1 "q") 'redis-cljs.core/brpop! :client]
                                                [#(core/brpop! client "") 'redis-cljs.core/brpop! :keys]
                                                [#(core/brpop! client "q" {:timeout-s -1}) 'redis-cljs.core/brpop! :timeout-s]
                                                [#(core/brpop! client "q" {:buffer-size 0}) 'redis-cljs.core/brpop! :buffer-size]
                                                [#(core/brpop! client "q" {:timeout 1}) 'redis-cljs.core/brpop! :opts]
                                                [#(core/blpop! client [] nil) 'redis-cljs.core/blpop! :keys]
                                                [#(core/feeds 1) 'redis-cljs.core/feeds :client]
                                                [#(core/wrap-client {:a 1} false) 'redis-cljs.core/wrap-client :js-client]
                                                [#(core/wrap-client (js-obj) false) 'redis-cljs.core/wrap-client :js-client]
                                                [#(core/wrap-client (:js client) "no") 'redis-cljs.core/wrap-client :pooled?]]]
                  (let [d (thrown-data f)]
                    (is (= :redis-cljs/argument-error (:type d)) (str function " " argument))
                    (is (= function (:function d)))
                    (is (= argument (:argument d))))))
              (is (= [] (fake/commands server)) "nothing was sent"))))))))
