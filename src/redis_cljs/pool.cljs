(ns redis-cljs.pool
  "Client pools: a pool is built from the same kebab-case configuration as
   a client (redis-cljs.core/connect!) plus the pool keys (:minimum
   :maximum :acquire-timeout-ms :cleanup-delay-ms), runs one-off commands
   on whichever client is free (command!), and lends a dedicated client
   for the duration of a function (with-client!): the place for a
   blocking pop or a MULTI / EXEC block. A client lent by with-client! is
   the SAME handle type as a connect! client: redis-cljs.core/command!,
   brpop! and redis-cljs.pubsub work on it unchanged, and
   redis-cljs.core/end! refuses it (the pool returns it). One handle is
   kept per upstream client for the life of the pool, so a loan attaches
   no listener of its own.

   Conventions, as in redis-cljs.core:
   - names ending in `!` perform I/O and return a promise; pool! is the
     exception: it allocates the pool (no socket) and returns the handle
     synchronously;
   - a pool handle is opaque (printing shows host, port, database, the
     limits and the live counters, never the password);
   - every public function checks its arguments first (an argument-error),
     a configuration is refused as a config-error before anything is sent,
     and the other rejections use the one taxonomy: :redis-cljs/network-error
     when a client cannot be obtained or the pool is gone (refused, a wait
     for a free client that timed out, used after end!),
     :redis-cljs/api-error when the server answers with an error reply;
   - upstream names that would collide with redis-cljs.core when both
     namespaces are referred into one (connect!, command!, end!) have a
     pool-prefixed alias: pool-connect!, pool-command!, pool-end!,
     pool-status;
   - the pool's `error` events are recorded on the handle (last-error) so
     that a pooled connection breaking never throws an unhandled error."
  (:require [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as core]
            [redis-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; The handle
;; ---------------------------------------------------------------------------

(defrecord Pool
  ;; js: the upstream node-redis client pool. state: atom {:config (the
  ;; validated configuration, redacted when printed) :last-error e
  ;; :handles {js-client handle} (one client handle per upstream client,
  ;; dropped when that client ends)}. The counters and the open / closing
  ;; flags are read live off the upstream object.
  [js state])

(defn pool?
  "True for a pool handle produced by pool!; false for anything else."
  [x]
  (instance? Pool x))

(defn check-pool!
  "Throws an argument-error (:argument :pool of `function`) unless `pool`
   is a pool handle. Returns `pool`."
  [function pool]
  (util/check! function :pool pool? "a pool handle" pool))

(defn status
  "The live state of the pool, synchronously: {:total-clients n (every
   client, idle or lent) :idle-clients n :waiting-tasks n (with-client! and
   command! calls queued because every client is busy) :open? (connect!
   done and end! not called) :closing? :minimum :maximum}. The counters are
   upstream's totalClients / idleClients / tasksQueueLength at the moment
   of the call; :minimum and :maximum are the configured limits (upstream's
   defaults 1 and 100 when absent). Throws an argument-error for a
   non-pool."
  [pool]
  (check-pool! 'redis-cljs.pool/status pool)
  (let [{:keys [minimum maximum] :or {minimum 1 maximum 100}} (:config @(:state pool))]
    (assoc (b/pool-counts (:js pool)) :minimum minimum :maximum maximum)))

(extend-protocol IPrintWithWriter
  Pool
  (-pr-writer [pool w _]
    (let [{:keys [open? closing?] :as counts} (b/pool-counts (:js pool))
          facts (merge (select-keys (util/redacted (:config @(:state pool))) [:host :port :database :url :maximum])
                       (select-keys counts [:total-clients :idle-clients])
                       {:status (cond closing? :closing open? :open :else :closed)})]
      (-write w (str "#redis-cljs/pool" (pr-str facts))))))

(defn last-error
  "The last `error` event the pool emitted (a pooled connection breaking),
   as a :redis-cljs/network-error ex-info, or nil. Throws an argument-error
   for a non-pool."
  [pool]
  (check-pool! 'redis-cljs.pool/last-error pool)
  (:last-error @(:state pool)))

(defn- ->network-error [op e]
  (util/normalize-failure op b/error-info e))

(defn- ->command-failure [op cmd e]
  (util/normalize-failure op b/error-info e {:command cmd}))

;; ---------------------------------------------------------------------------
;; Construction
;; ---------------------------------------------------------------------------

(defn pool!
  "Creates a pool and returns its handle at once, without connecting.
   `config` is a map of redis-cljs.util/pool-config-keys (every connect!
   key, handed to each client the pool creates, plus :minimum :maximum
   :acquire-timeout-ms :cleanup-delay-ms) or (from-env). Upstream
   defaults apply to absent pool keys: :minimum 1, :maximum 100,
   :acquire-timeout-ms 3000 (0 waits without limit), :cleanup-delay-ms
   3000. Throws a config-error synchronously for an unknown key, a wrong
   value (:minimum above :maximum included) or a configuration without
   :url or :host, its :received redacted. The clients are opened by
   connect! (the first :minimum) and on demand up to :maximum; idle
   clients above :minimum are closed after :cleanup-delay-ms. Each client
   is configured as a connect! client is (:reconnect false unless set)."
  [config]
  (let [cfg (-> config util/pool-config util/normalize-config util/check-pool-config!)
        [client-options pool-options] (util/pool-config->upstream cfg)
        js-pool (b/make-pool (b/config->js client-options) (b/config->js pool-options))
        handle (->Pool js-pool (atom {:config cfg :handles {}}))]
    (b/on js-pool "error" (fn [e] (swap! (:state handle) assoc :last-error (->network-error :event e))))
    handle))

;; ---------------------------------------------------------------------------
;; Lifecycle and use
;; ---------------------------------------------------------------------------

(defn connect!
  "Connects the pool's first :minimum clients and resolves with the pool
   handle once they are ready. Rejects with :redis-cljs/network-error when
   a socket fails (refused, timed out) and :redis-cljs/api-error when the
   server refuses the handshake, exactly as redis-cljs.core/connect! does;
   with an argument-error for a non-pool. Required before command! and
   with-client!: upstream rejects them on a pool that is not open (\"The
   client is closed\", a network-error)."
  [pool]
  (util/run-op (partial ->network-error :connect)
               (fn []
                 (check-pool! 'redis-cljs.pool/connect! pool)
                 (p/then (b/pool-connect (:js pool)) (fn [_] pool)))))

(defn command!
  "Runs one command on whichever client is free and resolves with the
   same reply as redis-cljs.core/command! (a string, a number, nil, a
   Buffer with {:buffers? true}, or a vector of these, with
   :redis-cljs/warnings metadata for an EXEC with failed elements), for
   the same `cmd` vector and `opts`; the same argument-errors apply.
   Upstream lends a client (opening one when every client is busy and the
   pool is below :maximum, or queueing the call until one is free, up to
   :acquire-timeout-ms), runs the one command and returns it; the promise
   settles once the client is back in the pool, so a call issued right
   after another reuses the client just freed. Not for blocking pops,
   MULTI / EXEC or WATCH: successive command! calls may run on different
   clients; use with-client!. Rejects as redis-cljs.core/command! does
   (:redis-cljs/api-error for an error reply, :redis-cljs/network-error
   when the connection is gone) plus :redis-cljs/network-error when no
   client could be obtained (\"Timeout waiting for a client after N ms\")
   or the pool has been ended (\"The client is closed\")."
  ([pool cmd] (command! pool cmd nil))
  ([pool cmd opts]
   (util/run-op (partial ->command-failure :command cmd)
                (fn []
                  (check-pool! 'redis-cljs.pool/command! pool)
                  (util/check-command! 'redis-cljs.pool/command! cmd)
                  (util/check-options! 'redis-cljs.pool/command! util/command-options opts)
                  (let [js-args (b/command->js (util/command->upstream 'redis-cljs.pool/command! cmd b/wire-object?))
                        js-opts (b/command-options->js opts)]
                    (p/then (b/pool-send-command (:js pool) js-args js-opts)
                            (fn [reply]
                              (let [v (b/reply->data reply)]
                                (if-let [warnings (util/warnings-of b/error-element? v)]
                                  (with-meta v {:redis-cljs/warnings (vec warnings)})
                                  v)))))))))

(defn- handle-of
  "The one client handle of upstream client `js-client` in this pool,
   created at its first loan and dropped when the client ends."
  [pool js-client]
  (or (get-in @(:state pool) [:handles js-client])
      (let [handle (core/wrap-client js-client true)]
        (swap! (:state pool) assoc-in [:handles js-client] handle)
        (core/on-end! handle (fn [] (swap! (:state pool) update :handles dissoc js-client)))
        handle)))

(defn with-client!
  "Lends a client of the pool to `f`, a function of the client handle
   returning a promise (or a value), and resolves with what `f` resolved
   with once the client is back in the pool (status then counts it idle
   again). The client is the caller's alone until then: run a blocking pop
   (redis-cljs.core/brpop!, taking from the channel inside `f` and
   stopping the feed with stop-feed! before `f` resolves: a feed left
   running keeps popping on a client that is no longer the caller's), a
   MULTI / EXEC block or a WATCH on it. The return happens whether `f`
   resolves, rejects or throws, and the rejection is re-thrown after it;
   an ex-info of caller code passes through untouched. Rejects with an
   argument-error for a non-pool or a non-function, and with
   :redis-cljs/network-error when no client could be obtained in
   :acquire-timeout-ms or the pool has been ended."
  [pool f]
  (util/run-op (partial ->network-error :with-client)
               (fn []
                 (check-pool! 'redis-cljs.pool/with-client! pool)
                 (util/check! 'redis-cljs.pool/with-client! :f fn? "a function of the client handle" f)
                 (b/pool-execute (:js pool)
                                 (fn [js-client]
                                   (p/create (fn [resolve reject]
                                               (try
                                                 (let [r (f (handle-of pool js-client))]
                                                   (if (p/promise? r)
                                                     (p/handle r (fn [v e] (if (some? e) (reject e) (resolve v))))
                                                     (resolve r)))
                                                 (catch :default e (reject e))))))))))

(defn end!
  "Closes every client of the pool, waiting for the commands in flight
   (a lent client is closed once returned), and resolves with nil. After
   end! every command! and with-client! rejects with a network-error
   (\"The client is closed\"), status reports :open? false, and the pool
   cannot be reopened. A second end! resolves nil at once. Rejects with an
   argument-error for a non-pool."
  [pool]
  (util/run-op (partial ->network-error :end)
               (fn []
                 (check-pool! 'redis-cljs.pool/end! pool)
                 (if (:open? (b/pool-counts (:js pool)))
                   (p/then (b/pool-close (:js pool)) (fn [_] nil))
                   nil))))

;; ---------------------------------------------------------------------------
;; Aliases for callers referring both redis-cljs.core and redis-cljs.pool
;; ---------------------------------------------------------------------------

(def pool-connect!
  "Alias of connect!, the same function: (pool-connect! pool) resolves
   with the pool handle once its first :minimum clients are ready, and
   rejects as connect! does. The name that does not collide with
   redis-cljs.core/connect!."
  connect!)

(def pool-command!
  "Alias of command!, the same function: (pool-command! pool cmd) and
   (pool-command! pool cmd opts) resolve with the reply of the command
   run on a free client (a string, a number, nil, a Buffer with
   {:buffers? true}, or a vector of these), and reject as command! does.
   The name that does not collide with redis-cljs.core/command!."
  command!)

(def pool-end!
  "Alias of end!, the same function: (pool-end! pool) resolves with nil
   once every client is closed, and rejects as end! does. The name that
   does not collide with redis-cljs.core/end!."
  end!)

(def pool-status
  "Alias of status, the same function: (pool-status pool) returns
   {:total-clients :idle-clients :waiting-tasks :open? :closing? :minimum
   :maximum}, and throws as status does."
  status)
