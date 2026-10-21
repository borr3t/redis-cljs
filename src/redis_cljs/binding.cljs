(ns redis-cljs.binding
  "The single JavaScript-interop namespace of redis-cljs: it requires
   `redis` (node-redis) lazily from the project's node_modules, constructs
   clients and client pools, invokes connect / sendCommand / subscribe /
   unsubscribe / close / execute, reads the live flags and counters off
   the upstream objects, subscribes to their events, reads properties off
   upstream errors, reads the process environment, and converts the
   fixed-shape envelopes (the options object, a RESP reply, an error) to
   ClojureScript data.

   Internal: the namespaces above it (redis-cljs.core, pool, pubsub,
   helpers) are the API; nothing here is part of the documented surface,
   and every function returns upstream's own value (native promises, JS
   objects, JS errors) that those namespaces turn into promesa promises
   and ex-info errors.

   What it never converts: the bytes of a value. A reply's bulk string is
   the string (or, on request, the Buffer) upstream decoded; an integer
   reply is the number upstream decoded; an argument is handed to upstream
   as the string or Buffer it is (a BigInt becomes its decimal text, the
   one representation RESP has for it). No JSON, no number parsing."
  (:require [redis-cljs.util :as util]))

;; ---------------------------------------------------------------------------
;; Module loading
;; ---------------------------------------------------------------------------

(defn node?
  "True when the runtime is Node.js (process.versions.node exists)."
  []
  (and (exists? js/process)
       (exists? js/process.versions)
       (exists? js/process.versions.node)))

(defn- resolve-project-module
  "Absolute path of a module under the current working directory's
   node_modules, so `redis` resolves from the consuming project even when
   the compiled script lives elsewhere."
  [module]
  (let [path (js/require "path")]
    (.resolve path (js/process.cwd) "node_modules" module)))

(defonce ^:private redis-cache (atom nil))

(defn redis-module
  "The `redis` module object, required once from the project's
   node_modules. Throws a :redis-cljs/config-error outside Node.js."
  []
  (when-not (node?)
    (throw (util/config-error 'redis-cljs.binding/redis-module :runtime "Node.js (`redis` is a Node package)" "browser")))
  (or @redis-cache
      (reset! redis-cache (js/require (resolve-project-module "redis")))))

(defn redis-version
  "The installed `redis` package version string, nil outside Node.js."
  []
  (when (node?)
    (unchecked-get (js/require (resolve-project-module "redis/package.json")) "version")))

;; ---------------------------------------------------------------------------
;; Property access
;; ---------------------------------------------------------------------------

(defn- prop
  "Property `k` of JS object `o`; nil for nil / undefined / absent."
  [o k]
  (when (some? o)
    (let [v (unchecked-get o k)]
      (when-not (undefined? v) v))))

(defn- prop-in
  [o ks]
  (reduce prop o ks))

;; ---------------------------------------------------------------------------
;; Process
;; ---------------------------------------------------------------------------

(defn env
  "The process environment as a map of variable name -> string, nil
   outside Node.js."
  []
  (when (node?)
    (let [e js/process.env]
      (into {} (map (fn [k] [k (unchecked-get e k)])) (js/Object.keys e)))))

(defn set-exit-code!
  "Sets the exit code of the process (`process.exitCode`); returns nil."
  [code]
  (set! (.-exitCode js/process) code)
  nil)

;; ---------------------------------------------------------------------------
;; Values
;; ---------------------------------------------------------------------------

(defn bigint?
  "True for a JavaScript BigInt value."
  [x]
  (= "bigint" (js* "typeof ~{}" x)))

(defn ->bigint
  "`BigInt(s)` for a string of decimal digits with an optional leading
   minus. Throws an argument-error for anything else, so upstream's
   SyntaxError never escapes."
  [s]
  (util/check! 'redis-cljs.binding/->bigint :s
               #(and (string? %) (re-matches #"-?[0-9]+" %))
               "a string of decimal digits with an optional leading minus" s)
  (js/BigInt s))

(defn buffer?
  "True for a Node Buffer."
  [x]
  (js/Buffer.isBuffer x))

(defn wire-object?
  "True for a value that goes on the wire as it is besides strings: a
   Buffer or a BigInt. The command translation passes anything else that
   is not a string, keyword or number through this test and refuses it."
  [x]
  (or (buffer? x) (bigint? x)))

(defn js-object?
  "True for a JavaScript object (an upstream client, say): not nil, not a
   ClojureScript collection, record or primitive."
  [x]
  (and (some? x)
       (= "object" (js* "typeof ~{}" x))
       (not (coll? x))
       (not (record? x))))

;; ---------------------------------------------------------------------------
;; CLJS -> JS (configuration, commands)
;; ---------------------------------------------------------------------------

(defn- plain->js
  "A string-keyed map -> a JS object; nested string-keyed maps are
   converted the same way; every other value is set as it is."
  [m]
  (let [o (js-obj)]
    (doseq [[k v] m]
      (unchecked-set o k (if (map? v) (plain->js v) v)))
    o))

(defn config->js
  "The upstream-keyed, nested config map of redis-cljs.util/config->upstream
   -> the options object handed to `createClient`."
  [upstream-config]
  (plain->js upstream-config))

(defn command->js
  "The argument vector of redis-cljs.util/command->upstream -> the JS array
   handed to `sendCommand`: strings and Buffers as they are, a BigInt as
   its decimal text."
  [args]
  (let [arr (array)]
    (doseq [a args]
      (.push arr (if (bigint? a) (.toString a) a)))
    arr))

(def ^:private blob-string-type
  "RESP_TYPES.BLOB_STRING: the type code of a bulk string in upstream's
   type mapping (the character code of `$`)."
  36)

(defn command-options->js
  "The options object of `sendCommand` for a command! options map: with
   :buffers? true a typeMapping that yields every bulk string as a Buffer;
   nil otherwise."
  [{:keys [buffers?]}]
  (when buffers?
    (let [mapping (js-obj)]
      (unchecked-set mapping (str blob-string-type) js/Buffer)
      (js-obj "typeMapping" mapping))))

;; ---------------------------------------------------------------------------
;; Client construction and invocation (upstream values returned as they are)
;; ---------------------------------------------------------------------------

(defn make-client
  "`redis.createClient(options)`. Never connects. Throws upstream's
   synchronous errors (a URL that does not parse: a TypeError)."
  [js-options]
  (js-invoke (redis-module) "createClient" js-options))

(defn connect
  "`client.connect()`: the native promise, resolving with the client once
   the handshake (AUTH, SELECT, CLIENT SETNAME, CLIENT SETINFO) is done."
  [client]
  (js-invoke client "connect"))

(defn send-command
  "`client.sendCommand(args, options)`: the native promise of the raw
   reply (a string, a number, null, a Buffer with the Buffer type mapping,
   or an array of those)."
  [client js-args js-options]
  (if (some? js-options)
    (js-invoke client "sendCommand" js-args js-options)
    (js-invoke client "sendCommand" js-args)))

(defn close
  "`client.close()`: the native promise resolving with undefined once the
   pending commands have been answered and the socket closed."
  [client]
  (js-invoke client "close"))

(defn destroy
  "`client.destroy()`: synchronous; rejects every pending command with
   upstream's DisconnectsClientError and destroys the socket. Returns nil."
  [client]
  (js-invoke client "destroy")
  nil)

(defn subscribe
  "`client.subscribe(channel, listener, buffers?)`: the native promise
   resolving with undefined once the server confirmed the subscription.
   `listener` is called with (message, channel), strings or Buffers."
  [client channel listener buffers?]
  (js-invoke client "subscribe" channel listener (boolean buffers?)))

(defn unsubscribe
  "`client.unsubscribe(channel, listener, buffers?)`: the native promise
   resolving once the server confirmed. With `listener` nil every listener
   of the channel is removed and UNSUBSCRIBE is sent."
  [client channel listener buffers?]
  (if (some? listener)
    (js-invoke client "unsubscribe" channel listener (boolean buffers?))
    (js-invoke client "unsubscribe" channel)))

(defn client-flags
  "The live flags of a client as data: {:open? :ready? :pub-sub?}
   (isOpen: connect was called and the client is not closed; isReady: the
   socket is connected and the handshake done; isPubSubActive: at least one
   subscription is live)."
  [client]
  {:open? (boolean (prop client "isOpen"))
   :ready? (boolean (prop client "isReady"))
   :pub-sub? (boolean (prop client "isPubSubActive"))})

(defn client-info
  "The non-secret connection facts of a client, read off its resolved
   options: {:host :port :database :name} for the ones present (the
   password and the URL are not read)."
  [client]
  (let [options (prop client "options")]
    (into {}
          (keep (fn [[k path]]
                  (let [v (prop-in options path)]
                    (when (some? v) [k v]))))
          {:host ["socket" "host"]
           :port ["socket" "port"]
           :database ["database"]
           :name ["name"]})))

;; ---------------------------------------------------------------------------
;; Pool construction and invocation
;; ---------------------------------------------------------------------------

(defn make-pool
  "`redis.createClientPool(clientOptions, poolOptions)`. Never connects."
  [js-client-options js-pool-options]
  (js-invoke (redis-module) "createClientPool" js-client-options js-pool-options))

(defn pool-connect
  "`pool.connect()`: the native promise resolving once :minimum clients are
   connected."
  [pool]
  (js-invoke pool "connect"))

(defn- after-return
  "`native-promise` settled one macrotask later: upstream returns a lent
   client to the idle set in a `finally` that runs AFTER the task's
   promise resolved, so a caller continuing at once would find the pool
   without that client (and open another). The hop lets the return happen
   first; the value and the rejection pass through unchanged."
  [native-promise]
  (js/Promise. (fn [resolve reject]
                 (.then native-promise
                        (fn [v] (js/setImmediate (fn [] (resolve v))))
                        (fn [e] (js/setImmediate (fn [] (reject e))))))))

(defn pool-send-command
  "`pool.sendCommand(args, options)`: the native promise of the raw reply,
   run on whichever client is free, settled once that client is back in
   the pool (see after-return)."
  [pool js-args js-options]
  (after-return
   (if (some? js-options)
     (js-invoke pool "sendCommand" js-args js-options)
     (js-invoke pool "sendCommand" js-args))))

(defn pool-execute
  "`pool.execute(fn)`: the native promise of what `f` (a function of the
   checked-out upstream client, returning a promise) resolved with,
   settled once the client is back in the pool (see after-return).
   Upstream recognizes a pending task only by a NATIVE Promise
   (`instanceof Promise`), and a promesa promise is a thenable of its own
   class, so the result of `f` is wrapped in one here."
  [pool f]
  (after-return
   (js-invoke pool "execute"
              (fn [client]
                (js/Promise. (fn [resolve reject]
                               (.then (f client) resolve reject)))))))

(defn pool-close
  "`pool.close()`: the native promise resolving once every client of the
   pool has been closed."
  [pool]
  (js-invoke pool "close"))

(defn pool-counts
  "The live counters of a pool as data: {:total-clients :idle-clients
   :waiting-tasks :open? :closing?} (totalClients, idleClients,
   tasksQueueLength, isOpen, isClosing)."
  [pool]
  {:total-clients (prop pool "totalClients")
   :idle-clients (prop pool "idleClients")
   :waiting-tasks (prop pool "tasksQueueLength")
   :open? (boolean (prop pool "isOpen"))
   :closing? (boolean (prop pool "isClosing"))})

;; ---------------------------------------------------------------------------
;; Events
;; ---------------------------------------------------------------------------

(defn on
  "`emitter.on(event, f)` on a client or a pool; returns f (the listener to
   remove later)."
  [emitter event f]
  (js-invoke emitter "on" event f)
  f)

(defn once
  "`emitter.once(event, f)`; returns f."
  [emitter event f]
  (js-invoke emitter "once" event f)
  f)

(defn off
  "`emitter.removeListener(event, f)`; returns nil. Works for a listener
   attached with once too."
  [emitter event f]
  (js-invoke emitter "removeListener" event f)
  nil)

(defn listener-count
  "`emitter.listenerCount(event)`."
  [emitter event]
  (js-invoke emitter "listenerCount" event))

;; ---------------------------------------------------------------------------
;; Errors
;; ---------------------------------------------------------------------------

(defn error-reply?
  "True when `e` is an upstream ErrorReply: the server answered a command
   with an error (`-WRONGTYPE ...`, `-ERR ...`)."
  [e]
  (let [ctor (prop (redis-module) "ErrorReply")]
    (and (some? ctor) (instance? ctor e))))

(defn error-info
  "The facts of a JS error, as data: :message, :name (the error class),
   the socket fields (:code :errno :syscall :address :port) when present,
   and :error-reply? true for a server error reply."
  [e]
  (let [reply? (error-reply? e)
        base (cond-> {:message (or (prop e "message") (str e))}
               (string? (prop-in e ["constructor" "name"])) (assoc :name (prop-in e ["constructor" "name"]))
               reply? (assoc :error-reply? true))]
    (into base
          (keep (fn [[js-name k]]
                  (let [v (prop e js-name)]
                    (when (some? v) [k v]))))
          [["code" :code] ["errno" :errno] ["syscall" :syscall] ["address" :address] ["port" :port]])))

;; ---------------------------------------------------------------------------
;; JS -> CLJS (replies)
;; ---------------------------------------------------------------------------

(defn reply->data
  "A raw reply -> data: an array becomes a vector (recursively), null
   becomes nil, a string, number or Buffer stays what it is. An ErrorReply
   inside an array (an element of a MULTI / EXEC reply) becomes its
   error-info map, marked :error-reply? true; redis-cljs.core/command!
   turns those into :redis-cljs/warnings metadata."
  [r]
  (cond
    (nil? r) nil
    (array? r) (mapv reply->data r)
    (instance? js/Error r) (error-info r)
    :else r))

(defn error-element?
  "True for an element of a converted reply that was an error reply."
  [x]
  (and (map? x) (true? (:error-reply? x))))
