(ns redis-cljs.util
  "Pure helpers shared by redis-cljs: the explicit translation tables between
   kebab-case keywords and the option names `redis` (node-redis) spells, the
   validation of configuration maps, command vectors and function
   arguments, the structured error constructors of the one redis-cljs error
   taxonomy, the environment reader's parsing, and the promise adapter
   every I/O namespace wraps its upstream calls in.

   Pure: no I/O and no JavaScript interop. Turning the translated maps into
   JS objects, reading properties off JS errors and converting replies is
   interop and lives in the single binding namespace (redis-cljs.binding);
   this namespace deals in strings, keywords, maps and vectors only. Two
   exceptions: run-op, the promise adapter, is plumbing, not I/O; and
   parse-url decodes the percent-escapes of a URL's credentials with
   decodeURIComponent, a pure function of a string, because node-redis
   does.

   Values are never reinterpreted here: a key, a field or a value given to
   a command travels as the string or Buffer it is; a reply comes back as
   the string, Buffer, number, nil or vector the server sent. No JSON, no
   number parsing, no keywordizing of keys.

   Secrets: no message of a config-error or argument-error ever contains
   a value of the caller's; the data carries the value under :received,
   redacted when it is a configuration (the password, the password part
   of a URL, the TLS key and passphrase)."
  (:require [clojure.string :as str]
            [promesa.core :as p]))

;; ---------------------------------------------------------------------------
;; Translation tables: redis-cljs kebab keyword -> upstream option path.
;; node-redis nests the socket options under `socket`, so a table entry is
;; the PATH of the option in the options object (a vector of names).
;; ---------------------------------------------------------------------------

(def client-config-keys
  "Configuration keys accepted by connect! (and pool!), each with the path
   of the upstream node-redis client option it becomes. Millisecond options
   end in -ms; upstream reads them as milliseconds too.

   | key                    | upstream                      | note                                              |
   | :url                   | url                           | redis[s]://[[username][:password]@][host][:port][/db]; its parts override the keys below |
   | :host                  | socket.host                   | a non-blank string                                |
   | :port                  | socket.port                   | 1..65535; default 6379 (upstream)                 |
   | :tls                   | socket.tls                    | true for TLS; a rediss:// URL sets it too         |
   | :tls-options           | socket.*                      | a map of tls-option-keys, merged into the socket options |
   | :username              | username                      | ACL user; AUTH is sent when either credential is present |
   | :password              | password                      | never printed by a handle, never carried in an error |
   | :database              | database                      | a non-negative integer; SELECT is sent when non-zero |
   | :name                  | name                          | CLIENT SETNAME, shown in CLIENT LIST              |
   | :connect-timeout-ms    | socket.connectTimeout         | default 5000 (upstream)                           |
   | :reconnect             | socket.reconnectStrategy      | false (default), true (upstream's backoff), a delay in ms, or (fn [retries cause] -> ms, false or an Error) |
   | :socket-timeout-ms     | socket.socketTimeout          | idle time without data after which the socket is closed; default none |
   | :ping-interval-ms      | pingInterval                  | PING sent periodically while idle; default none   |
   | :disable-offline-queue | disableOfflineQueue           | reject commands while disconnected instead of queueing them |

   One of :url or :host is required: the library never connects to a
   default host. A key not in this table is a config-error before anything
   is sent."
  {:url ["url"]
   :host ["socket" "host"]
   :port ["socket" "port"]
   :tls ["socket" "tls"]
   :tls-options ["socket"]
   :username ["username"]
   :password ["password"]
   :database ["database"]
   :name ["name"]
   :connect-timeout-ms ["socket" "connectTimeout"]
   :reconnect ["socket" "reconnectStrategy"]
   :socket-timeout-ms ["socket" "socketTimeout"]
   :ping-interval-ms ["pingInterval"]
   :disable-offline-queue ["disableOfflineQueue"]})

(def tls-option-keys
  "Keys accepted inside a :tls-options map and the Node `tls.connect` option
   each becomes (upstream spreads them into the socket options). Values
   (certificate text, Buffers) pass through; :key and :passphrase are
   secrets and are redacted wherever a configuration is printed or carried
   in an error."
  {:reject-unauthorized "rejectUnauthorized"
   :ca "ca"
   :key "key"
   :cert "cert"
   :servername "servername"
   :passphrase "passphrase"
   :ciphers "ciphers"
   :min-version "minVersion"
   :max-version "maxVersion"})

(def pool-only-keys
  "Configuration keys accepted by pool! on top of client-config-keys, each
   with the upstream node-redis pool option it becomes.

   | key                  | upstream        | default upstream                                  |
   | :minimum             | minimum         | 1 (clients kept connected; at least 1)            |
   | :maximum             | maximum         | 100 (clients at most; at least :minimum)          |
   | :acquire-timeout-ms  | acquireTimeout  | 3000 (wait for a free client; 0 waits without limit) |
   | :cleanup-delay-ms    | cleanupDelay    | 3000 (idle time after which a client above :minimum is closed) |"
  {:minimum "minimum"
   :maximum "maximum"
   :acquire-timeout-ms "acquireTimeout"
   :cleanup-delay-ms "cleanupDelay"})

(def pool-config-keys
  "Every configuration key pool! accepts: client-config-keys plus
   pool-only-keys."
  (merge client-config-keys pool-only-keys))

(def fixed-upstream-options
  "Options the library sets on every client and exposes as no key:
   RESP 2 (the protocol every Redis since 2.0 speaks and the one the
   blocking and pub/sub paths of this library are written against;
   node-redis 6 would default to RESP3), and maintNotifications
   \"disabled\" (a Redis Enterprise feature that upstream supports under
   RESP3 only, and whose handshake resolves the host name through DNS)."
  {"RESP" 2
   "maintNotifications" "disabled"})

(def default-config
  "The values of the keys the library defaults differently from upstream:
   :reconnect false (upstream retries forever with a backoff, so a
   connect! to a host that does not answer would never settle). An
   explicit key in the configuration overrides it."
  {:reconnect false})

(def env-keys
  "The environment variables from-env reads and the configuration key each
   fills: REDIS_URL -> :url, REDIS_HOST -> :host, REDIS_PORT -> :port (an
   integer), REDIS_USERNAME -> :username, REDIS_PASSWORD -> :password,
   REDIS_DATABASE -> :database (an integer), REDIS_TLS -> :tls (one of
   env-booleans). Every value goes through unwrap-env-value first."
  {"REDIS_URL" :url
   "REDIS_HOST" :host
   "REDIS_PORT" :port
   "REDIS_USERNAME" :username
   "REDIS_PASSWORD" :password
   "REDIS_DATABASE" :database
   "REDIS_TLS" :tls})

(def env-booleans
  "The spellings REDIS_TLS accepts, case-insensitively, and the boolean
   each means. Anything else is a config-error: a typo must not pass as
   \"off\"."
  {"1" true "true" true "yes" true "on" true
   "0" false "false" false "no" false "off" false})

;; ---------------------------------------------------------------------------
;; Errors: ONE taxonomy, :type under :redis-cljs/
;; ---------------------------------------------------------------------------

(def error-types
  "Every :type a redis-cljs ex-info can carry.
   - :redis-cljs/config-error — a configuration map that is not usable: an
     unknown key, a value of the wrong shape, neither :url nor :host, a
     malformed :url, a runtime that is not Node.js. Raised before anything
     is sent. Data: :message :function :argument :expected :received
     (redacted: no password, no TLS key or passphrase), plus :unknown and
     :allowed for an unknown key.
   - :redis-cljs/argument-error — an argument of a public function has the
     wrong shape: a non-client handle, a command that is not a vector, a
     blank channel name, an option of the wrong type. Raised before
     anything is sent. Data: :message :function :argument :expected
     :received, plus :index for a command argument and :unknown / :allowed
     for an option.
   - :redis-cljs/network-error — the connection failed or is gone: refused,
     :connect-timeout-ms elapsed, the socket closed, the client used after
     end!, the pool used after end! or a wait for a free client that timed
     out. Data: :message :cause :op plus :code (the socket error code, such
     as \"ECONNREFUSED\") and :name (the upstream error class) when present.
   - :redis-cljs/api-error — the server answered a command with an error
     reply. Data: :message (the server's text verbatim, such as
     \"WRONGTYPE Operation against a key holding the wrong kind of value\"),
     :code (its first word: \"WRONGTYPE\", \"ERR\", \"NOAUTH\", \"WRONGPASS\"
     ...), :cause, :op, :command (the command as sent).
   A message never contains a value of the caller's: the values are in the
   data."
  #{:redis-cljs/config-error :redis-cljs/argument-error
    :redis-cljs/network-error :redis-cljs/api-error})

(defn- make-error [type message data]
  (ex-info (str "redis-cljs: " message)
           (merge {:type type :message message} data)))

(defn config-error
  "ex-info of :type :redis-cljs/config-error for the configuration key or
   argument `argument` of `function` (a symbol): `expected` describes the
   shape wanted, `received` the value given (redact it first when it may
   hold a secret: the constructors below do). `more` adds :unknown,
   :allowed. The message names the function, the argument and the
   expectation, never the value."
  [function argument expected received & [more]]
  (make-error :redis-cljs/config-error
              (str function ": " (name argument) " must be " expected)
              (merge {:function function :argument argument :expected expected :received received}
                     more)))

(defn argument-error
  "ex-info of :type :redis-cljs/argument-error for the argument `argument`
   (a keyword) of `function` (a symbol): `expected` describes the shape
   wanted, `received` is the value given. `more` adds :index, :unknown,
   :allowed. The message names the function, the argument and the
   expectation, never the value."
  [function argument expected received & [more]]
  (make-error :redis-cljs/argument-error
              (str function ": " (name argument) " must be " expected)
              (merge {:function function :argument argument :expected expected :received received}
                     more)))

(defn network-error
  "ex-info of :type :redis-cljs/network-error. `data` carries :cause, :op
   and the upstream fields present."
  [message & [data]]
  (make-error :redis-cljs/network-error message data))

(defn api-error
  "ex-info of :type :redis-cljs/api-error. `data` carries :code, :cause,
   :op and :command."
  [message & [data]]
  (make-error :redis-cljs/api-error message data))

(defn error-data
  "The structured data of a redis-cljs error, nil when `e` is not one."
  [e]
  (when-let [d (ex-data e)]
    (when (contains? error-types (:type d))
      d)))

(defn error-type
  "One of error-types, or nil when `e` is not a redis-cljs error."
  [e]
  (:type (error-data e)))

(defn check!
  "Throws an argument-error for `argument` of `function` unless
   `(pred received)`; returns `received`. The validator every public
   function runs on its arguments before any I/O."
  [function argument pred expected received & [more]]
  (when-not (pred received)
    (throw (argument-error function argument expected received more)))
  received)

;; ---------------------------------------------------------------------------
;; Redaction
;; ---------------------------------------------------------------------------

(def ^:private url-credentials-pattern
  "The credentials of a URL: everything between :// and the LAST @ of the
   authority, the part before the first /, ? or # (node-redis reads the
   authority the same way: the last @ ends the credentials, so a username
   or a password holding @ is read whole)."
  #"://([^/?#]*)@")

(def ^:private url-scheme-pattern
  "The scheme of a URL with its separator, redis:// or rediss://."
  #"^[^:/?#]*://")

(def ^:private stray-at-pattern
  "What follows the scheme when an @ sits PAST the authority: after the
   first /, ? or #. Such a string is no URL node-redis reads credentials
   from, yet a password written with a raw /, ? or # in it ends the
   authority early and puts its @ exactly here: where the credentials
   end is unknown, so nothing after the scheme is shown."
  #"^[^/?#]*[/?#][\s\S]*@")

(defn- stray-at?
  "Whether `url` holds an @ past its authority (see stray-at-pattern); the
   scheme, when there is one, is skipped first."
  [url]
  (let [scheme (or (re-find url-scheme-pattern url) "")]
    (boolean (re-find stray-at-pattern (subs url (count scheme))))))

(defn- masked-credentials
  "The credentials of a URL with their password part masked: what follows
   the FIRST colon, the separator of username and password, replaced by
   ***; credentials without a colon (a username alone) kept."
  [credentials]
  (if-let [i (str/index-of credentials ":")]
    (str (subs credentials 0 i) ":***")
    credentials))

(defn- redact-url
  "A URL string with its password masked, the whole of what follows the
   scheme masked when an @ sits past the authority (where the credentials
   would end is then unknown: redis://u:pass?word@h shows as
   redis://***, a string with no scheme as ***), and anything that is not
   a string masked whole (an invalid :url value may still hold a secret).
   A string without @ holds no credentials under any reading and is kept."
  [url]
  (cond
    (not (string? url)) "***"
    (stray-at? url) (str (re-find url-scheme-pattern url) "***")
    :else (str/replace url url-credentials-pattern
                       (fn [[_ credentials]] (str "://" (masked-credentials credentials) "@")))))

(defn- redact-tls-options
  "A :tls-options map with :key and :passphrase masked; anything that is
   not a map masked whole."
  [tls]
  (if (map? tls)
    (cond-> tls
      (contains? tls :key) (assoc :key "***")
      (contains? tls :passphrase) (assoc :passphrase "***"))
    "***"))

(defn redacted
  "The configuration without its secrets: :password replaced by \"***\",
   the password part of a :url hidden (everything after the scheme when
   an @ past the first /, ? or # leaves the credentials without a certain
   end), and :key and :passphrase of :tls-options replaced by \"***\" (a
   :url or :tls-options of another type masked whole), and the password
   part of a bare URL string hidden the same way.
   Applied wherever a configuration is printed (a handle) or carried in
   an error. Anything else is returned as it is."
  [config]
  (cond
    (map? config)
    (cond-> config
      (contains? config :password) (assoc :password "***")
      (contains? config :url) (update :url redact-url)
      (contains? config :tls-options) (update :tls-options redact-tls-options))

    ;; a bare string may be a URL with a password in it
    (string? config) (redact-url config)

    :else config))

(def ^:private secret-config-keys
  "Configuration keys whose value is never carried in an error."
  #{:password :tls-options :url})

(defn- redacted-value
  "The value of configuration key `k` as an error may carry it."
  [k v]
  (cond
    (= :url k) (redact-url v)
    (= :tls-options k) (redact-tls-options v)
    (contains? secret-config-keys k) "***"
    :else v))

;; ---------------------------------------------------------------------------
;; Configuration
;; ---------------------------------------------------------------------------

(defn- config-of
  [function x]
  (cond
    (nil? x) {}
    (map? x) x
    :else (throw (config-error function :config "a map (a URL goes under :url)" (redacted x)))))

(defn connect-config
  "The configuration map behind a connect! argument: nil -> {} (then
   check-config! refuses it for lacking :url and :host), a map -> itself.
   Anything else, a bare URL string included, is a config-error: the
   configuration is always a map, {:url \"redis://...\"} for a URL."
  [x]
  (config-of 'redis-cljs.core/connect! x))

(defn pool-config
  "The configuration map behind a pool! argument: the same two forms as
   connect-config (nil, a map); the config-error names
   redis-cljs.pool/pool!."
  [x]
  (config-of 'redis-cljs.pool/pool! x))

(defn normalize-config
  "Drops nil-valued entries of a config map and fills in default-config for
   the keys absent. nil -> the defaults. Values are not touched otherwise."
  [config]
  (merge default-config (into {} (remove (comp nil? val)) config)))

(defn- check-keys!
  [function argument allowed m]
  (when-not (map? m)
    (throw (config-error function argument "a map" (redacted m))))
  (let [unknown (remove #(contains? allowed %) (keys m))]
    (when (seq unknown)
      (throw (config-error function argument
                           (str "a map with keys among " (pr-str (vec (sort (keys allowed)))))
                           (redacted m)
                           {:unknown (set unknown) :allowed (set (keys allowed))}))))
  m)

(defn- check-value!
  [function k pred expected config]
  (when (and (contains? config k) (not (pred (get config k))))
    (throw (config-error function k expected (redacted-value k (get config k))))))

(defn- non-blank? [s]
  (and (string? s) (not (str/blank? s))))

(defn- port? [n]
  (and (integer? n) (<= 1 n 65535)))

(defn- non-negative-integer? [n]
  (and (integer? n) (not (neg? n))))

(defn- positive-integer? [n]
  (and (integer? n) (pos? n)))

(defn- non-negative-number? [n]
  (and (number? n) (not (neg? n))))

(defn- reconnect-value?
  [v]
  (or (boolean? v) (non-negative-number? v) (fn? v)))

(def ^:private digit-values
  {"0" 0 "1" 1 "2" 2 "3" 3 "4" 4 "5" 5 "6" 6 "7" 7 "8" 8 "9" 9})

(defn parse-integer
  "The non-negative integer written by a string of decimal digits, nil for
   anything else (an empty string, a sign, other characters). Pure."
  [s]
  (when (and (string? s) (re-matches #"[0-9]+" s))
    (reduce (fn [n c] (+ (* n 10) (get digit-values c))) 0 s)))

(def url-pattern
  "The shape of a :url, as node-redis reads it: redis:// or rediss://, an
   optional [username][:password]@ (the LAST @ before the path ends the
   credentials, so a password may hold @), a host with an optional :port,
   an optional /database, an optional ?query. Groups: scheme, credentials,
   host and port, database."
  #"^(rediss?)://(?:([^/?#]*)@)?([^/?#@]*)(?:/([^?#]*))?(?:\?.*)?$")

(defn- decode-component
  "A URL component with its percent-escapes decoded, as node-redis reads
   the username and the password (decodeURIComponent); nil for a
   malformed escape, which node-redis refuses."
  [s]
  (try (js/decodeURIComponent s) (catch :default _ nil)))

(defn parse-url
  "The parts of a :url as node-redis reads them: {:tls? b :username s
   :password s :host s :port n :database n}, the absent parts absent: an
   empty port (redis://h:) and an empty path (redis://h/) leave :port and
   :database absent, as upstream leaves them unspecified; the username
   and the password are percent-decoded (u%40name is u@name). nil when
   the string holds whitespace or does not match url-pattern, when a
   non-empty port or database is not a string of digits, when a
   credential holds a malformed percent-escape, or when the host is
   empty (a bracketed IPv6 host is kept with its brackets). Pure; the
   parsed parts are checked by check-config! like the explicit keys (a
   database too long for a number is refused there)."
  [url]
  (when (and (string? url) (not (re-find #"\s" url)))
    (when-let [[_ scheme credentials host-port database] (re-matches url-pattern url)]
      (let [[host port] (if (str/starts-with? host-port "[")
                          (let [[_ h p] (re-matches #"(\[[^\]]*\])(?::(.*))?" host-port)] [h p])
                          (let [i (str/last-index-of host-port ":")]
                            (if (nil? i) [host-port nil] [(subs host-port 0 i) (subs host-port (inc i))])))
            [username password] (when credentials
                                   (let [i (str/index-of credentials ":")]
                                     (if (nil? i) [credentials nil] [(subs credentials 0 i) (subs credentials (inc i))])))
            username-d (when (some? username) (decode-component username))
            password-d (when (some? password) (decode-component password))
            port-n (when (seq port) (parse-integer port))
            database-n (when (seq database) (parse-integer database))]
        (when (and (seq host)
                   (or (empty? port) (some? port-n))
                   (or (empty? database) (some? database-n))
                   (or (nil? username) (some? username-d))
                   (or (nil? password) (some? password-d)))
          (cond-> {:tls? (= "rediss" scheme) :host host}
            (some? username-d) (assoc :username username-d)
            (some? password-d) (assoc :password password-d)
            (some? port-n) (assoc :port port-n)
            (some? database-n) (assoc :database database-n)))))))

(defn- check-url!
  "The :url of `config`, parsed and checked as the explicit keys are: a
   config-error naming :url for a string that does not parse (a non-digit
   port or database, an empty host), :port for a port out of range,
   :database for a database out of range, :tls for a :tls key that
   contradicts the scheme."
  [function config]
  (when (contains? config :url)
    (let [url (:url config)
          parsed (parse-url url)]
      (when-not parsed
        (throw (config-error function :url
                             "a string of the form redis://[[username][:password]@]host[:port][/db] (rediss:// for TLS) with a host, a numeric port and a numeric database"
                             (redact-url url))))
      (when (and (contains? parsed :port) (not (port? (:port parsed))))
        (throw (config-error function :port "an integer between 1 and 65535 (in :url)" (:port parsed))))
      (when (and (contains? parsed :database) (not (non-negative-integer? (:database parsed))))
        (throw (config-error function :database "a non-negative integer (in :url)" (:database parsed))))
      (when (and (contains? config :tls) (not= (:tls config) (:tls? parsed)))
        (throw (config-error function :tls "consistent with the scheme of :url (rediss:// for true, redis:// for false)" (:tls config)))))))

(defn- check-values!
  [function config]
  (check-url! function config)
  (check-value! function :host non-blank? "a non-blank string" config)
  (check-value! function :port port? "an integer between 1 and 65535" config)
  (check-value! function :tls boolean? "true or false" config)
  (check-value! function :username string? "a string" config)
  (check-value! function :password string? "a string" config)
  (check-value! function :database non-negative-integer? "a non-negative integer" config)
  (check-value! function :name non-blank? "a non-blank string" config)
  (check-value! function :connect-timeout-ms non-negative-number? "a non-negative number of milliseconds" config)
  (check-value! function :socket-timeout-ms non-negative-number? "a non-negative number of milliseconds" config)
  (check-value! function :ping-interval-ms non-negative-number? "a non-negative number of milliseconds" config)
  (check-value! function :disable-offline-queue boolean? "true or false" config)
  (check-value! function :reconnect reconnect-value?
                "false, true, a delay in milliseconds or a function of [retries cause]" config)
  (when (contains? config :tls-options)
    (let [tls (:tls-options config)]
      (when-not (map? tls)
        (throw (config-error function :tls-options "a map" (redacted-value :tls-options tls))))
      (let [unknown (remove #(contains? tls-option-keys %) (keys tls))]
        (when (seq unknown)
          (throw (config-error function :tls-options
                               (str "a map with keys among " (pr-str (vec (sort (keys tls-option-keys)))))
                               (redacted-value :tls-options tls)
                               {:unknown (set unknown) :allowed (set (keys tls-option-keys))}))))))
  (when-not (or (string? (:url config)) (string? (:host config)))
    (throw (config-error function :config "a map with :url or :host (the library never connects to a default host)"
                         (redacted config)))))

(defn check-config!
  "Throws a config-error when `config` (already normalized) is not a map,
   has a key outside client-config-keys, a value of the wrong shape, a
   :tls-options map with a key outside tls-option-keys, a :url that does
   not parse or whose host, port, database or scheme fail the same checks
   as the explicit keys, or neither :url nor :host. Returns `config`.
   Upstream silently ignores unknown option names and connects to
   localhost by default; redis-cljs fails before anything is sent. The
   error's :received is redacted."
  [config]
  (let [function 'redis-cljs.core/connect!]
    (check-keys! function :config client-config-keys config)
    (check-values! function config)
    config))

(defn check-pool-config!
  "The pool counterpart of check-config!: keys outside pool-config-keys
   (client keys plus pool-only-keys) and the pool values are checked too
   (:minimum and :maximum positive integers, :minimum not above :maximum);
   the config-error names redis-cljs.pool/pool!. Returns `config`."
  [config]
  (let [function 'redis-cljs.pool/pool!]
    (check-keys! function :config pool-config-keys config)
    (check-values! function config)
    (check-value! function :minimum positive-integer? "a positive integer" config)
    (check-value! function :maximum positive-integer? "a positive integer" config)
    (check-value! function :acquire-timeout-ms non-negative-number? "a non-negative number of milliseconds" config)
    (check-value! function :cleanup-delay-ms non-negative-number? "a non-negative number of milliseconds" config)
    (let [{:keys [minimum maximum] :or {minimum 1 maximum 100}} config]
      (when (> minimum maximum)
        (throw (config-error function :maximum "an integer not below :minimum" maximum))))
    config))

(defn- reconnect->upstream
  "The reconnectStrategy value for a :reconnect value: false stays false
   (never reconnect), true becomes absent (upstream's own backoff), a number
   or a function passes through."
  [v]
  (if (true? v) ::absent v))

(defn config->upstream
  "A validated kebab config map -> a nested map keyed by the upstream option
   names (strings), as node-redis reads it: the socket options under
   \"socket\", :tls-options spread into it through tls-option-keys, every
   other value verbatim, plus fixed-upstream-options (RESP 2, maintenance
   notifications off). Pool-only keys are left out. A map that
   check-pool-config! refuses (an unknown key, a wrong value, a bad
   :tls-options, a pool key out of range) is an argument-error: the
   documented path validates first, this guards a direct call. The check
   runs on the configuration itself; only the error's :received is
   redacted."
  [config]
  (when-not (and (map? config)
                 (try (check-pool-config! config) true (catch :default _ false)))
    (throw (argument-error 'redis-cljs.util/config->upstream :config
                           "a configuration map that check-pool-config! accepts" (redacted config))))
  (reduce (fn [m [k v]]
            (if-let [path (get client-config-keys k)]
              (cond
                (= :tls-options k)
                (update m "socket" merge (into {} (map (fn [[tk tv]] [(get tls-option-keys tk) tv])) v))

                (= :reconnect k)
                (let [v (reconnect->upstream v)]
                  (if (= ::absent v) m (assoc-in m path v)))

                :else (assoc-in m path v))
              m))
          fixed-upstream-options
          config))

(defn pool-config->upstream
  "A validated pool config map -> [client-options pool-options]: the client
   options as config->upstream builds them, and the pool options keyed by
   the upstream names of pool-only-keys."
  [config]
  [(config->upstream (apply dissoc config (keys pool-only-keys)))
   (into {} (keep (fn [[k name]] (when (contains? config k) [name (get config k)]))) pool-only-keys)])

;; ---------------------------------------------------------------------------
;; Environment values
;; ---------------------------------------------------------------------------

(defn unwrap-env-value
  "Copy-paste defense for a string read from the environment: trims the
   whitespace around the value and strips ONE layer of matching quotes
   (double, single or back quotes) when it starts and ends with the same
   quote. What the quotes held is kept as it is, spaces included, and a
   quoted empty string is the empty string; nothing else is removed, so a
   password with a dash or a quote inside survives. nil for a blank or
   non-string input. from-env applies it to every variable it reads;
   values given in a configuration map are never touched."
  [v]
  (let [v (when (string? v) (str/trim v))]
    (when-not (str/blank? v)
      (let [n (count v)
            wrapped? (fn [q] (and (>= n 2) (str/starts-with? v q) (str/ends-with? v q)))]
        (if (or (wrapped? "\"") (wrapped? "'") (wrapped? "`"))
          (subs v 1 (dec n))
          v)))))

(defn env->config
  "The configuration map read from `env` (a map of variable name ->
   string, as the process environment gives it) through env-keys: each
   value unwrapped by unwrap-env-value, REDIS_PORT and REDIS_DATABASE
   parsed as non-negative integers, REDIS_TLS read through env-booleans
   (case-insensitively); a value that is none of these is a config-error
   naming the key. Blank variables are absent; a quoted empty string is
   the empty string. Pure:
   redis-cljs.core/from-env reads the real environment."
  [env]
  (let [function 'redis-cljs.core/from-env]
    (reduce (fn [config [variable k]]
              (if-some [v (unwrap-env-value (get env variable))]
                (assoc config k
                       (case k
                         (:port :database)
                         (or (parse-integer v)
                             (throw (config-error function k (str "an integer in " variable) v)))

                         :tls
                         (let [b (get env-booleans (str/lower-case v) ::none)]
                           (if (= ::none b)
                             (throw (config-error function k
                                                  (str "one of " (pr-str (vec (sort (keys env-booleans)))) " in " variable)
                                                  v))
                             b))

                         v))
                config))
            {}
            env-keys)))

;; ---------------------------------------------------------------------------
;; Commands as data
;; ---------------------------------------------------------------------------

(def command-function
  "The function argument errors of a command vector name."
  'redis-cljs.core/command!)

(defn- argument->upstream
  "One command argument -> what node-redis sends: a string as it is, a
   keyword as its upper-cased name (a modifier such as :ex or :nx), a
   number as its decimal text, and an object `wire-object?` accepts (the
   binding's Buffers and BigInts) as it is. Anything else (nil, booleans,
   collections, symbols, functions, other objects) is an argument-error:
   it has no RESP representation the caller wrote."
  [function wire-object? i x]
  (cond
    (string? x) x
    (keyword? x) (str/upper-case (name x))
    (number? x) (str x)
    (and (some? x) (not (boolean? x)) (not (coll? x)) (not (symbol? x)) (not (fn? x)) (wire-object? x)) x
    :else
    (throw (argument-error function :command
                           "a vector of strings, Buffers, numbers or keyword modifiers after the command name"
                           x {:index i}))))

(defn- command-name? [head]
  (or (keyword? head)
      (non-blank? head)
      (symbol? head)))

(defn check-command!
  "Throws an argument-error (for `function`, default command!) unless `cmd`
   is a non-empty vector (or list) whose first element names a command (a
   keyword, a non-blank string or a symbol). Returns `cmd`. A bare string
   is NOT accepted: a command is always a vector, [:set \"key\" \"value\"]."
  ([cmd] (check-command! command-function cmd))
  ([function cmd]
   (when-not (and (sequential? cmd) (seq cmd))
     (throw (argument-error function :command "a non-empty vector [:cmd \"arg\" ...]" cmd)))
   (when-not (command-name? (first cmd))
     (throw (argument-error function :command "a vector whose first element names the command (a keyword or a non-blank string)"
                            cmd {:index 0})))
   cmd))

(defn command->upstream
  "A validated command vector -> the vector of arguments node-redis sends:
   the command name upper-cased ([:hset ...] -> \"HSET\"; a multi-word name
   such as \"CLIENT ID\" or :client-id is split into its words; a name
   that is blank once normalized, such as :- or (keyword \"\"), is an
   argument-error), then every argument through the rules of
   argument->upstream: strings as they are, keywords upper-cased, numbers
   as decimal text, and the objects `wire-object?` accepts (none by
   default; the core passes the binding's test for Buffers and BigInts)
   as they are. Throws an argument-error with :index for any other
   argument."
  ([cmd] (command->upstream command-function cmd))
  ([function cmd] (command->upstream function cmd (constantly false)))
  ([function cmd wire-object?]
   (let [head (first cmd)
         words (-> (if (keyword? head) (str/replace (name head) "-" " ") (str head))
                   str/upper-case
                   str/trim
                   (str/split #"\s+"))]
     (when (str/blank? (str/join " " words))
       (throw (argument-error function :command "a vector whose first element names the command (a keyword or a non-blank string)"
                              cmd {:index 0})))
     (into (vec words)
           (map-indexed (fn [i x] (argument->upstream function wire-object? (inc i) x)))
           (rest cmd)))))

(def command-options
  "Options of command!: :buffers? (true or false) makes every bulk string
   of the reply a Buffer instead of a string (the arguments are sent as
   given either way)."
  {:buffers? boolean?})

(defn- option-expectation
  "What an option value must be, by option key."
  [k]
  (case k
    (:buffers? :force?) "true or false"
    :timeout-s "a non-negative number of seconds"
    :buffer-size "a positive integer"
    "a value of the documented shape"))

(defn check-options!
  "Throws an argument-error for :opts of `function` unless `opts` is nil or
   a map whose keys are all in `allowed` (a map of option key -> predicate
   of its value) with every value accepted by its predicate; a wrong value
   names the option as :argument. Returns `opts`."
  [function allowed opts]
  (when-not (or (nil? opts) (map? opts))
    (throw (argument-error function :opts "a map of options or nil" opts)))
  (let [unknown (remove #(contains? allowed %) (keys opts))]
    (when (seq unknown)
      (throw (argument-error function :opts (str "a map with keys among " (pr-str (vec (sort (keys allowed)))))
                             opts {:unknown (set unknown) :allowed (set (keys allowed))}))))
  (doseq [[k pred] allowed]
    (when (and (contains? opts k) (not (pred (get opts k))))
      (throw (argument-error function k (option-expectation k) (get opts k)))))
  opts)

(defn check-command-options!
  "Throws an argument-error unless `opts` is nil or a map of
   command-options with boolean values. Returns `opts`."
  [opts]
  (check-options! command-function command-options opts))

(defn keys-argument
  "The vector of key names behind the `keys` argument of a blocking pop: a
   string -> [s], a vector of strings -> itself; anything else is an
   argument-error for `function`."
  [function keys]
  (let [ks (if (string? keys) [keys] keys)]
    (when-not (and (vector? ks) (seq ks) (every? non-blank? ks))
      (throw (argument-error function :keys "a key name or a non-empty vector of key names (non-blank strings)" keys)))
    ks))

;; ---------------------------------------------------------------------------
;; Failure classification
;; ---------------------------------------------------------------------------

(defn error-reply-code
  "The first word of a Redis error reply, the error's code by convention:
   \"WRONGTYPE\" for \"WRONGTYPE Operation against ...\", \"ERR\" for
   \"ERR unknown command ...\"."
  [message]
  (first (str/split (str message) #"\s+" 2)))

(defn failure-kind
  "Given the facts read off an error that rejected an operation
   (:error-reply? :code :message), :api when the server answered with an
   error reply and :network otherwise (the connection refused, timed out,
   closed, the client closed, a pool wait that timed out)."
  [{:keys [error-reply?]}]
  (if error-reply? :api :network))

(defn passthrough-error?
  "True when `e` already is a redis-cljs error or any other ex-info (caller
   code): those are rethrown untouched; only JS errors are normalized."
  [e]
  (or (some? (error-data e)) (instance? ExceptionInfo e)))

(defn normalize-failure
  "The ex-info for a rejection `e` of operation `op`: a passthrough error
   as it is; otherwise an api-error when the facts say the server answered
   with an error reply, a network-error otherwise. `info-of` reads the
   error's facts (redis-cljs.binding/error-info); the data is those facts
   plus :cause (the JS error), :op and `extra` (such as :command)."
  [op info-of e & [extra]]
  (if (passthrough-error? e)
    e
    (let [info (info-of e)
          data (merge (dissoc info :error-reply?) {:cause e :op op} extra)]
      (if (= :api (failure-kind info))
        (api-error (:message info) (assoc data :code (error-reply-code (:message info))))
        (network-error (:message info) data)))))

(defn warnings-of
  "The :redis-cljs/warnings of a reply vector whose elements include error
   replies (a MULTI / EXEC block with a failed command): one map
   {:index i :code \"WRONGTYPE\" :message \"...\"} per error element, in
   order; nil when none. `error-element?` recognizes an element as an
   error (the binding marks them with :error-reply? true)."
  [error-element? v]
  (when (vector? v)
    (seq (keep-indexed (fn [i x]
                         (when (error-element? x)
                           {:index i :code (error-reply-code (:message x)) :message (:message x)}))
                       v))))

;; ---------------------------------------------------------------------------
;; The promise adapter shared by the I/O namespaces
;; ---------------------------------------------------------------------------

(defn run-op
  "Runs `f` (which may throw synchronously and returns a promise or a
   value) and returns a promesa promise settled with its outcome, every
   rejection passed through `normalize`. Built with p/create around the
   upstream promise, never chained off a shared resolved promise."
  [normalize f]
  (p/create
   (fn [resolve reject]
     (try
       (let [r (f)]
         (if (p/promise? r)
           (p/handle r (fn [v e]
                         (if (some? e)
                           (reject (normalize e))
                           (resolve v))))
           (resolve r)))
       (catch :default e
         (reject (normalize e)))))))
