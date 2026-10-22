(ns redis-cljs.examples
  "The examples of the README, runnable OFFLINE:

     npx shadow-cljs compile examples && node target/examples.js

   The library does all it would do for a Redis server: it validates the
   configuration, constructs the real node-redis client or pool, which
   runs the whole protocol over TCP (the handshake, pipelined commands,
   blocking pops, pub/sub), and converts every reply, error and message
   into data. What it talks to is a fake: the scripted RESP2 server of the
   test suite (redis-cljs.fake-server) on a free port of the loopback
   interface, answering each command from a table. No Redis server, no
   network beyond 127.0.0.1, no password.

   What stands between a README:begin and a README:end mark stands in
   README.md as written, and what `main` prints stands in README.md as
   printed (test/redis_cljs/examples_test.cljs holds the README to it and
   runs every function of this file)."
  (:require [cljs.core.async :as async]
            [cljs.pprint :as pprint]
            [clojure.string :as str]
            [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as redis]
            [redis-cljs.fake-server :as fake]
            [redis-cljs.helpers :as helpers]
            [redis-cljs.pool :as pool]
            [redis-cljs.pubsub :as pubsub]
            [redis-cljs.util :as util])
  (:require-macros [redis-cljs.doctest :refer [claims]]))

;; ---------------------------------------------------------------------------
;; Quick start: the configuration
;; ---------------------------------------------------------------------------

;; README:begin config
(def config
  "The connection settings of the examples: the keys of a node-redis
   client, kebab-cased. A real server adds :password (or :username and
   :password), or the whole thing is one URL under :url."
  {:host "127.0.0.1"
   :port 6379
   :name "redis-cljs-examples"})

(def url-config
  "The same connection as a URL: redis://[[username][:password]@][host][:port][/db],
   rediss:// for TLS. The parts of the URL override the other keys."
  {:url  "redis://127.0.0.1:6379/0"
   :name "redis-cljs-examples"})
;; README:end config

;; ---------------------------------------------------------------------------
;; The fake server: what it answers, by command
;; ---------------------------------------------------------------------------

(defn script
  "What the fake server answers, by command name: the replies a Redis
   would give to the commands of this page, in the order they are sent.
   A function, so that every server starts the in-order replies afresh."
  []
  {"HSET"    2
   "INCR"    1
   "HGETALL" ["name" "pencil" "price" "150"]
   "GET"     nil
   "RPUSH"   2
   "BLPOP"   (fake/in-order ["jobs" "invoice-41"] ["jobs" "invoice-42"] nil)
   "TTL"     86400
   "HGET"    (fake/error "WRONGTYPE Operation against a key holding the wrong kind of value")})

(defn fake-server
  "A fake server answering from `script`; a promise of its handle."
  []
  (fake/start! {:script (script)}))

(defn offline
  "`config` pointed at `server`: the port of the fake instead of 6379 (and
   the URL of the fake for a URL configuration)."
  [server config]
  (if (:url config)
    (update config :url str/replace "127.0.0.1:6379" (str "127.0.0.1:" (fake/port server)))
    (assoc config :port (fake/port server))))

(defn- port-of
  "The port an upstream options object would connect to."
  [upstream-config]
  (or (get-in upstream-config ["socket" "port"])
      (some->> (get upstream-config "url") (re-find #":(\d+)(?:/|$)") second util/parse-integer)))

(defn no-server!
  "Installs a tripwire where the library turns a configuration into the
   options of a node-redis client: from here on, a configuration that does
   not point at a live fake server (or at a port the fake released, which
   refuses) is refused. An example that forgot its fake fails at once
   instead of opening a socket to a real Redis."
  []
  (let [real b/config->js]
    (set! b/config->js
          (fn [upstream-config]
            ;; the pool options object (minimum, maximum ...) names no host
            (when (or (contains? upstream-config "socket") (contains? upstream-config "url"))
              (let [port (port-of upstream-config)]
                (when-not (or (contains? (fake/released-ports) port) (contains? (fake/live-ports) port))
                  (throw (ex-info "offline: no connection is opened here; the config needs the port of a fake server"
                                  {:type :redis-cljs.examples/offline :port port})))))
            (real upstream-config)))))

;; ---------------------------------------------------------------------------
;; Quick start
;; ---------------------------------------------------------------------------

;; README:begin quick-start
(defn quick-start
  "Connects, stores a product as a hash and counts a view, reads them
   back, ends the connection. Resolves with what the server answered."
  [config]
  (p/let [client  (redis/connect! config)
          _       (redis/command! client [:hset "product:1" "name" "pencil" "price" "150"])
          views   (redis/command! client [:incr "product:1:views"])
          product (redis/command! client [:hgetall "product:1"])
          missing (redis/command! client [:get "product:9"])
          _       (redis/end! client)]
    {:views   views
     :product (helpers/pairs->map product)
     :missing missing}))
;; README:end quick-start

(defn quick-start-offline
  "`quick-start` against the fake server, with what the server received."
  []
  (p/let [server (fake-server)
          result (quick-start (offline server config))
          _      (fake/stop! server)]
    (assoc result :sent (fake/commands server))))

(defn url-offline
  "A connection from the URL form, naming database 2, against the fake
   server: what the server received (the SELECT of the database the URL
   names is sent by node-redis itself)."
  []
  (p/let [server (fake-server)
          client (redis/connect! (offline server (assoc url-config :url "redis://127.0.0.1:6379/2")))
          pong   (redis/command! client [:ping])
          _      (redis/end! client)
          _      (fake/stop! server)]
    {:pong pong :sent (fake/commands server)}))

;; ---------------------------------------------------------------------------
;; Scenario: a work queue over BLPOP
;; ---------------------------------------------------------------------------

;; README:begin queue
(defn first-of
  "A promise of the first value taken from the core.async channel `ch`
   (nil once the channel is closed)."
  [ch]
  (p/create (fn [resolve _] (async/take! ch resolve))))

(defn work-queue
  "A producer pushes two jobs at the tail of the list; a worker, on its
   own connection, pops them from the head through a BLPOP channel (RPUSH
   and BLPOP make a FIFO queue), stops the feed once it has two, and
   drains what the stop left. Resolves with the queue length after the
   push, the jobs in the order they were popped, and what the channel
   yields once it is closed."
  [producer worker]
  (p/let [queued (redis/command! producer [:rpush "jobs" "invoice-41" "invoice-42"])
          jobs   (redis/blpop! worker "jobs" {:timeout-s 1})
          first  (first-of jobs)
          second (first-of jobs)
          _      (redis/stop-feed! worker jobs)
          then   (first-of jobs)]
    {:queued queued
     :popped [first second]
     :then   then}))
;; README:end queue

(defn work-queue-offline
  "`work-queue` with two connections to the fake server, whose BLPOP
   answers two jobs and then the timeout."
  []
  (p/let [server   (fake-server)
          producer (redis/connect! (offline server config))
          worker   (redis/connect! (offline server config))
          result   (work-queue producer worker)
          _        (redis/end! producer)
          _        (redis/end! worker)
          _        (fake/stop! server)]
    (assoc result :sent (fake/commands server))))

;; ---------------------------------------------------------------------------
;; Scenario: pub/sub across two connections
;; ---------------------------------------------------------------------------

;; README:begin pubsub
(defn announce
  "Subscribes `subscriber` to the \"news\" channel, has `publisher`
   (another connection) publish on it, takes the message and
   unsubscribes. Resolves with the receiver count PUBLISH reported, the
   message, and what the subscriber still subscribes to."
  [subscriber publisher]
  (p/let [news      (pubsub/subscribe! subscriber "news")
          receivers (pubsub/publish! publisher "news" "price drop on pencils")
          message   (first-of news)
          _         (pubsub/unsubscribe! subscriber "news")]
    {:receivers        receivers
     :message          message
     :still-subscribed (pubsub/subscribed subscriber)}))
;; README:end pubsub

(defn announce-offline
  "`announce` with two connections to the fake server, which delivers a
   PUBLISH to the connections subscribed to its channel as a server does."
  []
  (p/let [server     (fake-server)
          subscriber (redis/connect! (offline server config))
          publisher  (redis/connect! (offline server config))
          result     (announce subscriber publisher)
          _          (redis/end! subscriber)
          _          (redis/end! publisher)
          _          (fake/stop! server)]
    (assoc result :sent (fake/commands server))))

;; ---------------------------------------------------------------------------
;; Scenario: a pool
;; ---------------------------------------------------------------------------

;; README:begin pool
(defn pooled
  "One-off commands on whichever pooled client is free, and a blocking
   pop on a client lent for the duration of a function: the feed pops
   until the queue has stayed empty for a second and closes itself, so
   the loan ends with nothing in flight. Resolves with the replies and
   the counters of the pool afterwards."
  [pool]
  (p/let [_      (pool/connect! pool)
          ttl    (pool/command! pool [:ttl "product:1"])
          popped (pool/with-client! pool
                   (fn [client]
                     (let [jobs (redis/blpop! client "jobs" {:timeout-s 1})]
                       (p/let [first  (first-of jobs)
                               second (first-of jobs)
                               closed (first-of jobs)]
                         [first second closed]))))
          status (pool/status pool)
          _      (pool/end! pool)]
    {:ttl    ttl
     :popped popped
     :status (select-keys status [:total-clients :idle-clients :maximum])}))
;; README:end pool

(defn pooled-offline
  "`pooled` with a pool of at most two clients against the fake server."
  []
  (p/let [server (fake-server)
          result (pooled (pool/pool! (offline server (assoc config :maximum 2))))
          _      (fake/stop! server)]
    (assoc result :sent (fake/commands server))))

;; ---------------------------------------------------------------------------
;; Error handling
;; ---------------------------------------------------------------------------

;; README:begin errors
(defn outcome
  "The value of `(f)`, or its failure as data: the :type of every
   redis-cljs rejection and the fields that matter for it. An error reply
   of the server carries its text verbatim in :message and its first word
   in :code; a connection failure carries the socket error code in :code
   when there is one, its message otherwise."
  [f]
  (-> (p/let [value (f)] {:ok? true :value value})
      (p/catch (fn [e]
                 (let [{:keys [type] :as data} (ex-data e)]
                   (case type
                     :redis-cljs/api-error      {:ok? false :why :server
                                                 :error (select-keys data [:code :message :command])}
                     :redis-cljs/network-error  {:ok? false :why :connection
                                                 :error (select-keys data (if (:code data) [:code] [:message]))}
                     :redis-cljs/config-error   {:ok? false :why :configuration
                                                 :error (select-keys data [:argument :unknown])}
                     :redis-cljs/argument-error {:ok? false :why :argument
                                                 :error (select-keys data [:function :argument])}
                     (throw e)))))))
;; README:end errors

(defn errors-offline
  "`outcome` of a HGET on a key that holds a string, of a command that is
   not a vector, of a configuration with a key the client does not have,
   of a connection to a port where nothing listens, and of a command after
   end!."
  []
  (p/let [server      (fake-server)
          closed-port (fake/closed-port)
          client      (redis/connect! (offline server config))
          wrong-type  (outcome #(redis/command! client [:hget "product:1:views" "name"]))
          not-vector  (outcome #(redis/command! client "GET product:1"))
          unknown-key (outcome #(redis/connect! (offline server (assoc config :timeout 5))))
          refused     (outcome #(redis/connect! (assoc config :port closed-port)))
          _           (redis/end! client)
          after-end   (outcome #(redis/command! client [:get "product:1"]))
          _           (fake/stop! server)]
    {:wrong-type  wrong-type
     :not-vector  not-vector
     :unknown-key unknown-key
     :refused     refused
     :after-end   after-end}))

;; ---------------------------------------------------------------------------
;; Pure helpers: the one-liners of the README, each with its value
;; ---------------------------------------------------------------------------

(def one-liners
  "The one-liners of the README as [source value] pairs: the form as it
   is written and what it evaluates to."
  (claims
   (util/command->upstream [:set "k" "v" :ex 60 :nx])
   (helpers/pairs->map ["name" "pencil" "price" "150"])
   (helpers/map->pairs {"name" "pencil" "price" "150"})
   (helpers/pop-reply ["jobs" "invoice-41"])
   (helpers/scan-reply ["17" ["product:1" "product:2"]])
   (helpers/ok? "OK")
   (util/config->upstream (util/normalize-config {:host "127.0.0.1" :port 6379 :database 2}))
   (util/redacted {:host "cache.example.test" :password "hunter2"})
   (util/unwrap-env-value "\"s3cret-\"")
   (sort util/error-types)
   (redis/client? config)
   redis/api-version
   pubsub/default-buffer-size))

;; ---------------------------------------------------------------------------
;; Runner
;; ---------------------------------------------------------------------------

(defn printed
  "What `main` prints for the result `value` of the example `title`: the
   title as a comment, then the value pretty-printed. README.md shows
   this text as it is."
  [title value]
  (str ";; " title "\n"
       ;; pprint leaves a space at the end of some lines; none is kept
       (str/replace (with-out-str (pprint/pprint value)) #" +\n" "\n")))

(defn printed-one-liners
  "What `main` prints for the one-liners: each form, then `;; =>` and its
   value, a blank line between them. README.md shows this text as it is."
  []
  (str/join "\n" (map (fn [[source value]] (str source "\n;; => " (pr-str value) "\n")) one-liners)))

(defn main [& _]
  (no-server!)
  (-> (p/let [started   (quick-start-offline)
              by-url    (url-offline)
              queued    (work-queue-offline)
              announced (announce-offline)
              pooled    (pooled-offline)
              failed    (errors-offline)]
        (print (printed "quick start" started))
        (print (printed "from a URL" by-url))
        (print (printed "a work queue over BLPOP" queued))
        (print (printed "pub/sub across two connections" announced))
        (print (printed "a pool" pooled))
        (print (printed "failures are data" failed))
        (println ";; pure helpers")
        (print (printed-one-liners)))
      (p/catch (fn [e]
                 (println "example failed:" (ex-message e) (pr-str (ex-data e)))
                 (b/set-exit-code! 1)))))
