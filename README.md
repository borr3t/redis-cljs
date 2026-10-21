# redis-cljs

A thin, faithful ClojureScript (Node.js) wrapper of
[node-redis](https://github.com/redis/node-redis) (the `redis` npm package): a Redis
client or pool is a configuration map, a command is a vector of data
(`[:set "key" "value"]`), and replies, errors and messages come back as data.
Configuration and messages are maps of kebab-cased keywords, asynchronous operations
return [promesa](https://github.com/funcool/promesa) promises, pub/sub messages and
blocking pops are core.async channels, and the library has no runtime npm dependency
beyond `redis`. Values are strings or Buffers in and out and are never reinterpreted:
no JSON, no number parsing, no keywordizing; an integer reply is a number because the
protocol types it so. Everything the library does happens in the process; only
`connect!` and the pool open a socket, to the server the configuration names.

## Requirements

Node.js 22+, Java 11+ for shadow-cljs. The library is consumed as a source dependency:

```clojure
;; deps.edn of the consuming app, next to a checkout:
{:deps {io.github.borr3t/redis-cljs {:local/root "../redis-cljs"}}}
```

The npm side is the consuming app's: `redis` 6.3.0 in its `package.json`, resolved from
its `node_modules` at run time. `deps.edn` declares ClojureScript, core.async and
promesa.

```json
{"dependencies": {"redis": "6.3.0"}}
```

The library speaks RESP2 to the server (node-redis 6 would default to RESP3): every
Redis since 2.0 answers it, and the replies this page shows are RESP2 shapes (a hash is
a flat vector of fields and values).

## Quick start (offline)

The quick start connects, writes a hash and a counter, reads them back and ends the
connection, all against a fake server: the real node-redis client runs the whole
protocol (the handshake, pipelined commands, the close) over TCP to a scripted server
on a free port of the loopback interface, and the library validates the configuration,
builds the commands and converts the replies exactly as it would for Redis. No Redis
server, no network beyond 127.0.0.1, no password.

```bash
npm install
npx shadow-cljs compile examples && node target/examples.js
```

That runs [`dev/redis_cljs/examples.cljs`](dev/redis_cljs/examples.cljs), which holds
every example of this page and prints every result shown below, exactly as shown. Its
namespace form:

```clojure
(ns redis-cljs.examples
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
```

The configuration of a client is a map of the node-redis client options, kebab-cased,
or a map holding a URL:

```clojure
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
```

A connection, a few commands, and the connection ended:

```clojure
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
```

`(quick-start config)` against the fake resolves with the first three entries; `:sent`
is what the fake server received from the real client. The examples print:

```clojure
;; quick start
{:views 1,
 :product {"name" "pencil", "price" "150"},
 :missing nil,
 :sent
 [["CLIENT" "SETNAME" "redis-cljs-examples"]
  ["HSET" "product:1" "name" "pencil" "price" "150"]
  ["INCR" "product:1:views"]
  ["HGETALL" "product:1"]
  ["GET" "product:9"]]}
```

The command name is upper-cased, a keyword argument becomes an upper-cased modifier
(`:ex`, `:nx`), a number becomes its decimal text, strings and Buffers go as they are.
`INCR` answers an integer, so `:views` is a number; `HGETALL` answers a flat vector of
fields and values, which `helpers/pairs->map` shapes into a map whose keys are the
exact field strings; `GET` of a missing key answers nil. `:name` is sent as
`CLIENT SETNAME`; a `:database` other than 0, or a `/2` at the end of a URL, is sent as
`SELECT`. With `url-config` naming database 2, against the fake:

```clojure
;; from a URL
{:pong "PONG",
 :sent
 [["CLIENT" "SETNAME" "redis-cljs-examples"] ["SELECT" "2"] ["PING"]]}
```

### Going live

The same code against a server of yours: give `connect!` a configuration with your
host, or a map with a `redis://` URL under `:url`, or `(redis/from-env)`, which reads
`REDIS_URL`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_USERNAME`, `REDIS_PASSWORD`,
`REDIS_DATABASE` and `REDIS_TLS` from the environment of the process (one layer of
copy-paste quotes stripped) and nothing else; keys merged over it override the
variables. **This connects to your own server with your own credentials and runs the
commands you give it: whether and when to do that is your decision.** The library
reads no file: keep the credentials in the environment of the process, or in a
`NAME=value` file outside the repository that only you can read and load with
`set -a; . <secrets-file>; set +a`. A client handle prints its host, port, database,
name and status, never the password; an error about a configuration carries the
configuration with its password, URL password, TLS key and passphrase replaced by
`***`, and its message names no value at all. A configuration without `:url` or
`:host` is refused before anything is sent: the library never connects to a default
host.

## Scenarios

Each scenario is a function of `dev/redis_cljs/examples.cljs`, shown with the result
it resolves with against the fake, as the examples print it. Given a configuration of
yours, the same functions run against Redis.

### A work queue over BLPOP

`blpop!` gives a core.async channel fed by BLPOP: each popped item arrives as a map,
BLPOP is sent again once the item has been taken into the channel's buffer, and the
feed ends when a pop times out, when `stop-feed!` stops it, or when the client ends.
`RPUSH` appends at the tail and `BLPOP` pops from the head, so the two make a
first-in, first-out queue. A blocking pop holds its connection, so the worker has one
of its own.

```clojure
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
```

With two connections to the fake, whose BLPOP answers the two jobs and then the
timeout, `(work-queue producer worker)` resolves with the first three entries:

```clojure
;; a work queue over BLPOP
{:queued 2,
 :popped
 [{:key "jobs", :value "invoice-41"}
  {:key "jobs", :value "invoice-42"}],
 :then nil,
 :sent
 [["CLIENT" "SETNAME" "redis-cljs-examples"]
  ["CLIENT" "SETNAME" "redis-cljs-examples"]
  ["RPUSH" "jobs" "invoice-41" "invoice-42"]
  ["BLPOP" "jobs" "1"]
  ["BLPOP" "jobs" "1"]
  ["BLPOP" "jobs" "1"]]}
```

Three BLPOPs went out: one per item, and the one in flight when `stop-feed!` was
called. The stop sends no further pop, waits for that one (here it timed out, since the
queue was empty; had it answered, the item would have been handed to the channel) and
closes the channel; what the channel holds stays takeable until exhausted, so the third
take yields nil and nothing popped is lost. The channel has a FIXED buffer of 1024
items (the `:buffer-size` option): when it is full the next pop waits for the consumer,
so a slow consumer leaves items in Redis instead of dropping them; a full buffer never
makes `stop-feed!` wait, since the item waiting for room stays takeable after the
close: drain until nil. `:timeout-s 0`, the default, pops without limit. Closing the
channel yourself also stops the feed but loses the item of the pop in flight: use
`stop-feed!`. `end!` stops the feeds the same way and resolves once their channels are
closed. An error (a WRONGTYPE reply, the connection gone) arrives on the channel as
`{:error e}` before it closes. `brpop!` is the same from the tail of the list.

### Pub/sub across two connections

`subscribe!` resolves with a core.async channel of the messages published on a name;
a PUBLISH from any connection, here a second one, arrives on it as a map. Only
messages reach the channel: node-redis keeps the protocol's `subscribe` and
`unsubscribe` confirmations to itself.

```clojure
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
```

With two connections to the fake, which delivers a PUBLISH to the connections
subscribed to its channel as a server does, `(announce subscriber publisher)` resolves
with the first three entries:

```clojure
;; pub/sub across two connections
{:receivers 1,
 :message {:channel "news", :message "price drop on pencils"},
 :still-subscribed #{},
 :sent
 [["CLIENT" "SETNAME" "redis-cljs-examples"]
  ["CLIENT" "SETNAME" "redis-cljs-examples"]
  ["SUBSCRIBE" "news"]
  ["PUBLISH" "news" "price drop on pencils"]
  ["UNSUBSCRIBE" "news"]]}
```

`:message` is the published value verbatim (a Buffer with `{:buffers? true}`, and so
is `:channel`), `:channel` the name as the server reports it. The channel has a
sliding buffer of 1024 messages (the `:buffer-size` option): it fills whether or not
anyone takes from it, and when it is full the oldest is dropped; nothing is replayed,
as Redis itself queues nothing for a subscriber that was not there. `unsubscribe!`
closes the channel and sends UNSUBSCRIBE; the channel also closes when the connection
ends. Closing the channel yourself stops delivery at the next message, when its
listener is detached and, if it was the last one for the name, UNSUBSCRIBE is sent. A
subscribed connection serves pub/sub only: subscribe on a `connect!` client of its own
and publish from another, or from a pool.

### A pool

A pool is built from the client configuration plus the pool keys and connects on
`connect!`. `pool/command!` runs one command on whichever client is free;
`with-client!` lends a client for the duration of a function and takes it back
whether the function resolves, rejects or throws; a blocking pop, a MULTI / EXEC block
or a WATCH belongs on such a lent client, never on `pool/command!`, whose successive
calls may run on different connections. A feed on a lent client must be finished
before the function resolves: stopped with `stop-feed!`, or, as here, left to close
itself on its timeout.

```clojure
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
```

With `(pool/pool! (assoc config :maximum 2))` against the fake, `(pooled pool)`
resolves with the first three entries:

```clojure
;; a pool
{:ttl 86400,
 :popped
 [{:key "jobs", :value "invoice-41"}
  {:key "jobs", :value "invoice-42"}
  nil],
 :status {:total-clients 1, :idle-clients 1, :maximum 2},
 :sent
 [["CLIENT" "SETNAME" "redis-cljs-examples"]
  ["TTL" "product:1"]
  ["BLPOP" "jobs" "1"]
  ["BLPOP" "jobs" "1"]
  ["BLPOP" "jobs" "1"]]}
```

One client served everything and sat idle in the pool afterwards until `end!` closed
it. The lent client is the same handle as a `connect!` client: `redis/command!`,
`redis/blpop!` and `redis-cljs.pubsub` work on it unchanged; `redis/end!` refuses it,
the pool returns it. The pool's promises settle once the client is back in the pool,
so `status` read right after a call counts it idle again, and the same handle is lent
every time: a loan attaches nothing to the client.

## Pure helpers

No server, no network. The examples print each form and its value:

```clojure
(util/command->upstream [:set "k" "v" :ex 60 :nx])
;; => ["SET" "k" "v" "EX" "60" "NX"]

(helpers/pairs->map ["name" "pencil" "price" "150"])
;; => {"name" "pencil", "price" "150"}

(helpers/map->pairs {"name" "pencil", "price" "150"})
;; => ["name" "pencil" "price" "150"]

(helpers/pop-reply ["jobs" "invoice-41"])
;; => {:key "jobs", :value "invoice-41"}

(helpers/scan-reply ["17" ["product:1" "product:2"]])
;; => {:cursor "17", :items ["product:1" "product:2"]}

(helpers/ok? "OK")
;; => true

(util/config->upstream (util/normalize-config {:host "127.0.0.1", :port 6379, :database 2}))
;; => {"RESP" 2, "maintNotifications" "disabled", "socket" {"reconnectStrategy" false, "host" "127.0.0.1", "port" 6379}, "database" 2}

(util/redacted {:host "cache.example.test", :password "hunter2"})
;; => {:host "cache.example.test", :password "***"}

(util/unwrap-env-value "\"s3cret-\"")
;; => "s3cret-"

(sort util/error-types)
;; => (:redis-cljs/api-error :redis-cljs/argument-error :redis-cljs/config-error :redis-cljs/network-error)

(redis/client? config)
;; => false

redis/api-version
;; => "0.1.0"

pubsub/default-buffer-size
;; => 1024
```

`util` is `redis-cljs.util`: the translation tables between the kebab-case keys and
the names node-redis spells, applied by `connect!`, `pool!` and `command!`.
`config->upstream` shows what the library adds on its own: `RESP 2`, no maintenance
notifications (a Redis Enterprise feature node-redis supports under RESP3 only), and
no reconnection (`reconnectStrategy false`) unless an explicit `:reconnect` key asks
for it. `unwrap-env-value` strips one layer of matching quotes from a value read from
the environment and nothing else; `from-env` applies it, configuration maps are never
touched. `helpers/big-int` builds a JavaScript BigInt for a 64-bit integer, to pass as
an argument (sent as its decimal text) without a BigInt literal in ClojureScript
source.

## Error handling

Every rejection is an `ex-info` whose data carries a `:type`. A bad configuration and
a bad argument are refused before anything is sent:

| `:type`                        | when                                                                                                                             | data                                                                                                  |
|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| `:redis-cljs/config-error`     | a configuration that is not a map, an unknown key, a value of the wrong shape (a blank host, a port outside 1..65535, a negative database, `:minimum` above `:maximum`), neither `:url` nor `:host`, a malformed `:url`, a `REDIS_PORT` or `REDIS_TLS` that does not parse, a runtime that is not Node.js | `:message` (names the key and the expectation, never the value), `:function`, `:argument`, `:expected`, `:received` (redacted), plus `:unknown` and `:allowed` for an unknown key |
| `:redis-cljs/argument-error`   | a non-client or non-pool handle, a command that is not a vector, has a blank name or holds a nil / boolean / collection / function, a blank channel name, bad keys for a pop, an option of the wrong type or an unknown one, a lent client given to `end!`, a channel that is not a feed | `:message`, `:function`, `:argument`, `:expected`, `:received`, plus `:index` for a command argument and `:unknown` / `:allowed` for an option |
| `:redis-cljs/network-error`    | the socket failed or the connection is gone: refused, `:connect-timeout-ms` elapsed, the socket closed, a client or pool used after `end!`, a wait for a free pooled client that timed out | `:message`, `:cause` (the node-redis error), `:op`; `:code` (`"ECONNREFUSED"` ...) for a socket error, `:name` the error class |
| `:redis-cljs/api-error`        | the server answered a command with an error reply (`WRONGTYPE`, `ERR`, `NOAUTH`, `WRONGPASS`, `NOPERM` ...)                     | `:message` the server's text verbatim, `:code` its first word, `:command` the vector as sent, `:cause`, `:op` |

A server rejection leaves the connection usable; a `network-error` means the handle
is finished (with `:reconnect` set, node-redis reconnects and the client reports
`:connecting` meanwhile): connect again. One reply resolves partially: an EXEC whose
block had a failed command resolves with the vector, the failed elements as maps, and
the failures as `:redis-cljs/warnings` metadata on the vector (`(meta reply)`). An
`ex-info` thrown by caller code inside `with-client!` passes through untouched. Every
function checks its arguments before any I/O; the synchronous ones (`status`,
`subscribed`, `brpop!`, `pool!` ...) throw, the others reject.

```clojure
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
```

For a HGET on a key that holds a string, a command given as a string instead of a
vector, a configuration with a key the client does not have, a connection to a port of
127.0.0.1 where nothing listens (one the fake bound and released), and a command after
`end!`, `outcome` resolves with:

```clojure
;; failures are data
{:wrong-type
 {:ok? false,
  :why :server,
  :error
  {:code "WRONGTYPE",
   :message
   "WRONGTYPE Operation against a key holding the wrong kind of value",
   :command [:hget "product:1:views" "name"]}},
 :not-vector
 {:ok? false,
  :why :argument,
  :error {:function redis-cljs.core/command!, :argument :command}},
 :unknown-key
 {:ok? false,
  :why :configuration,
  :error {:argument :config, :unknown #{:timeout}}},
 :refused
 {:ok? false, :why :connection, :error {:code "ECONNREFUSED"}},
 :after-end
 {:ok? false,
  :why :connection,
  :error {:message "The client is closed"}}}
```

`p/let` evaluates `(f)` inside a promise, so what the library throws synchronously
arrives at the same `p/catch` as what it rejects with. The `:message` of a
config-error or argument-error names the function, the argument and what was
expected, and never the value received, which stays in the data; the `:code` of a
network-error is the socket error code when there is one.

## Features

* **Clients** — `connect!` from a kebab-case map or the `REDIS_*` environment;
  `command!` with commands as vectors and `{:buffers? true}`; `brpop!` and `blpop!`
  as channel feeds with `stop-feed!`; `end!` (graceful, or `{:force? true}`, which
  also cuts a graceful `end!` still waiting); `status`, `last-error` and `feeds` read
  locally.
* **Replies as data** — a simple or bulk string as a string (or a Buffer on request),
  an integer as a number, a null as nil, an array as a vector; never reinterpreted; a
  mixed EXEC reply with its failures as metadata.
* **Pools** — `pool!` with `:minimum :maximum :acquire-timeout-ms :cleanup-delay-ms`;
  `connect!`; `command!` on a free client; `with-client!` for a dedicated one, the
  same handle every time; `status` with the live counters; `end!`. Aliases
  `pool-connect!`, `pool-command!`, `pool-end!`, `pool-status` for code that refers
  both namespaces.
* **Pub/sub as channels** — `subscribe!` gives a core.async channel per name with a
  sliding buffer; `unsubscribe!` closes it; `subscribed` lists the open names;
  `publish!` resolves with the receiver count.
* **Readable keys, untouched values** — configuration keys are translated through
  explicit tables to the names node-redis spells; command arguments and reply values
  pass through as they are.
* **Structured errors** — every rejection is an `ex-info` with a `:type` of one
  taxonomy; a server error reply carries its text verbatim and its code; no message
  carries a value of yours, no error carries a secret.
* **Validation up front** — an unknown configuration key, a wrong value, a malformed
  command or a wrong handle is refused before anything is sent, naming the function
  and the argument; node-redis itself would ignore an unknown option silently and
  connect to localhost by default.
* **Nothing decided for the caller** — no reconnection unless `:reconnect` asks for
  it, no command timeouts, no retries, no encoding of values; the client's `error`
  events are recorded on the handle instead of thrown, and every channel of a client
  closes when its connection is gone for good.
* **No interop at call sites** — the public namespaces carry no `^js` hints;
  JavaScript lives in one internal binding namespace, `redis-cljs.binding`.

## Limits and settings

The limits that matter are Redis's and node-redis's: the memory of the server, the
size of a value (512 MB), the number of clients the server allows, the timeouts the
server enforces. The library enforces none of them and invents none: it reports what
the server or node-redis refused. Every timeout below is a node-redis option, and
absent means node-redis's default.

Client configuration (`connect!`; every key is a pool key too):

| key                       | default                          | meaning                                                                     |
|---------------------------|----------------------------------|-----------------------------------------------------------------------------|
| `:url`                    | none                             | `redis://[[username][:password]@]host[:port][/db]`, `rediss://` for TLS; its parts override the keys below and are checked like them (a host, a port in range, an integer database, a scheme consistent with `:tls`) |
| `:host`                   | none; `:url` or `:host` is required | the server, a non-blank string                                           |
| `:port`                   | 6379                             | 1 to 65535                                                                  |
| `:tls`                    | `false`                          | `true` for TLS                                                              |
| `:tls-options`            | none                             | a map of `:reject-unauthorized :ca :key :cert :servername :passphrase :ciphers :min-version :max-version` |
| `:username`               | none                             | the ACL user; AUTH is sent when either credential is present                |
| `:password`               | none                             | a string; never printed by a handle, never carried in an error              |
| `:database`               | 0                                | a non-negative integer; SELECT is sent when non-zero                        |
| `:name`                   | none                             | CLIENT SETNAME, shown in CLIENT LIST                                        |
| `:connect-timeout-ms`     | 5000                             | how long `connect!` waits for the TCP connection                            |
| `:reconnect`              | `false`                          | `false`: the first failure is final and a broken connection stays broken; `true`: node-redis's own backoff, retried without limit; a number: a fixed delay in ms; a function of `[retries cause]` returning a delay, `false` or an Error |
| `:socket-timeout-ms`      | none                             | idle time without data after which the socket is closed                     |
| `:ping-interval-ms`       | none                             | PING sent periodically while idle (keeps a proxy from dropping the connection) |
| `:disable-offline-queue`  | `false`                          | reject commands while disconnected instead of queueing them until ready    |

Pool configuration (`pool!`, on top of the client keys):

| key                      | default   | meaning                                                                    |
|--------------------------|-----------|----------------------------------------------------------------------------|
| `:minimum`               | 1         | clients kept connected; at least 1                                         |
| `:maximum`               | 100       | clients at most; not below `:minimum`                                      |
| `:acquire-timeout-ms`    | 3000      | how long `command!` or `with-client!` waits for a free client; 0 waits without limit |
| `:cleanup-delay-ms`      | 3000      | idle time after which a client above `:minimum` is closed                 |

Channels:

| function      | option          | default | meaning                                                              |
|---------------|-----------------|---------|----------------------------------------------------------------------|
| `subscribe!`  | `:buffer-size`  | 1024    | messages held, sliding: when full, the oldest is dropped             |
| `subscribe!`  | `:buffers?`     | `false` | `:channel` and `:message` as Buffers                                 |
| `brpop!`, `blpop!` | `:timeout-s` | 0      | the BRPOP / BLPOP timeout in seconds; 0 blocks without limit         |
| `brpop!`, `blpop!` | `:buffer-size` | 1024 | items held, fixed: when full, the next pop waits for the consumer   |
| `brpop!`, `blpop!` | `:buffers?` | `false` | `:key` and `:value` as Buffers                                      |

The environment variables `from-env` reads:

| variable          | key          | note                                                 |
|-------------------|--------------|------------------------------------------------------|
| `REDIS_URL`       | `:url`       |                                                      |
| `REDIS_HOST`      | `:host`      |                                                      |
| `REDIS_PORT`      | `:port`      | an integer                                           |
| `REDIS_USERNAME`  | `:username`  |                                                      |
| `REDIS_PASSWORD`  | `:password`  |                                                      |
| `REDIS_DATABASE`  | `:database`  | an integer                                           |
| `REDIS_TLS`       | `:tls`       | `1`, `true`, `yes`, `on` for true; `0`, `false`, `no`, `off` for false; anything else is a config-error |

What the library does not do: it does not retry a command, does not reconnect unless
`:reconnect` says how, does not encode or decode values (a map to store is JSON the
caller wrote; a number to read back is the string the caller parses), does not
emulate RESP3 (a hash is a flat vector; use `helpers/pairs->map`); it sends one
command at a time in the order given on one connection (node-redis pipelines them),
so a blocking pop holds every command behind it: give it a connection of its own.
`end!` waits for the commands in flight, a blocking pop included; `{:force? true}`
cuts them, also when a graceful `end!` (or two) is still waiting. An integer reply
above 2^53 arrives as a JavaScript number and loses precision; store such values as
strings.

## Documentation

* [`docs/redis-cljs.org`](docs/redis-cljs.org) — the manual: the client handle,
  commands as data, promises and channels, the configuration translation, the
  client, pool and pub/sub guides, the gotchas, testing, the break-the-gate evidence,
  and the reference of every public function.
* [`docs/redis-api-inventory.org`](docs/redis-api-inventory.org) — the surface of
  Redis and of node-redis that the library wraps: the RESP2 reply types, every
  command in scope with its arguments, reply and edge cases, the client options,
  errors and events, with the source each fact was taken from.
* The docstrings of `src/redis_cljs/` are the reference of each function.

## Layout

```
src/redis_cljs/
  core.cljs            from-env, connect!, command!, brpop!, blpop!, stop-feed!, feeds, end!, status, last-error, on-end!, client?, api-version
  pool.cljs            pool!, connect!, command!, with-client!, status, last-error, end!, pool?
  pubsub.cljs          subscribe!, unsubscribe!, subscribed, publish!
  helpers.cljs         pairs->map, map->pairs, pop-reply, scan-reply, ok?, big-int
  util.cljs            the translation tables, validation, the error taxonomy, redaction, the environment parsing, the promise adapter
  binding.cljs         the one JavaScript namespace (internal): the redis module, clients, pools, events, errors, replies
dev/redis_cljs/
  examples.cljs        the examples of this page, offline
  dev.cljs             the Node runtime a REPL attaches to
test/redis_cljs/
  fake_server.cljs     the scripted RESP2 server the suite and the examples run against
docs/
  redis-cljs.org               the manual
  redis-api-inventory.org      the API inventory
```

## Public API index

* `redis-cljs.core`: `(from-env)`, `(connect! config)`, `(command! client cmd)`,
  `(command! client cmd opts)`, `(brpop! client keys)`, `(brpop! client keys opts)`,
  `(blpop! client keys)`, `(blpop! client keys opts)`, `(stop-feed! client ch)`,
  `(feeds client)`, `(end! client)`, `(end! client opts)`, `(status client)`,
  `(last-error client)`, `(on-end! client f)`, `(client? x)`,
  `(check-client! function x)`, `(wrap-client js-client pooled?)`, `api-version`,
  `default-buffer-size`, `pop-options`, `end-options`.
* `redis-cljs.pool`: `(pool! config)`, `(connect! pool)`, `(command! pool cmd)`,
  `(command! pool cmd opts)`, `(with-client! pool f)`, `(status pool)`,
  `(last-error pool)`, `(end! pool)`, `(pool? x)`, `(check-pool! function x)`, and
  the aliases `pool-connect!`, `pool-command!`, `pool-end!`, `pool-status`.
* `redis-cljs.pubsub`: `(subscribe! client channel-name)`, `(subscribe! client
  channel-name opts)`, `(unsubscribe! client channel-name)`, `(subscribed client)`,
  `(publish! client channel-name message)`, `default-buffer-size`,
  `subscribe-options`.
* `redis-cljs.helpers`: `(pairs->map pairs)`, `(map->pairs m)`, `(pop-reply reply)`,
  `(scan-reply reply)`, `(ok? reply)`, `(big-int x)`.
* `redis-cljs.util`: the tables `client-config-keys`, `tls-option-keys`,
  `pool-only-keys`, `pool-config-keys`, `fixed-upstream-options`, `default-config`,
  `env-keys`, `env-booleans`, `url-pattern`, `command-options`, `command-function`,
  `error-types`; `(config->upstream config)`, `(pool-config->upstream config)`,
  `(command->upstream cmd)`, `(redacted config)`, `(unwrap-env-value v)`,
  `(parse-integer s)`, `(parse-url url)`, `(env->config env)`, `(error-reply-code message)`,
  `(error-type e)`, `(error-data e)`, `(warnings-of error-element? v)`, the error
  constructors `config-error`, `argument-error`, `network-error`, `api-error`, the
  validators `connect-config`, `pool-config`, `normalize-config`, `check-config!`,
  `check-pool-config!`, `check-command!`, `check-command-options!`, `check-options!`,
  `check!`, `keys-argument`, and the plumbing `failure-kind`, `passthrough-error?`,
  `normalize-failure`, `run-op`.
* `redis-cljs.binding` is internal: the one namespace that touches JavaScript. Its
  functions return native promises and JavaScript objects and are not part of the
  documented surface.

## Development

```bash
npm install
./run-tests.sh     # the publicity gate, then the unit suite; loopback only
npx shadow-cljs compile examples && node target/examples.js   # the examples of this page; loopback only
npx shadow-cljs watch dev                                     # a Node runtime for the REPL
```

The suite runs the real node-redis client and pool against the scripted RESP2 fake
server on a free port of 127.0.0.1: configuration translation, validation and
redaction, connect, the handshake, commands and replies, error replies, a mixed EXEC
reply, expiry as the server reports it, blocking pops and their stop, pub/sub, pools
and their counters, ending (graceful, forced, and after a broken connection), the
argument contract of every public function, and the examples of this page: it fails
when this page shows a code block or a printed result that the examples do not
produce, character for character, or a code block the examples do not have, and when
the manual's API reference and the public functions of the source disagree. The one
socket the suite opens elsewhere goes to a port of 127.0.0.1 that the fake bound and
released, to prove a refused connection is reported. `./run-tests.sh` ends by
replacing its shell with node, so nothing can follow it on the same command line.

## License

[Unlicense](LICENSE).
