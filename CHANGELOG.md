# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0]

### Added

- The client surface: `connect!` (a kebab-case config map, a `redis://` URL under
  `:url`, or `(from-env)`), `command!` (commands as vectors, replies as the data the
  server sent, `:buffers?` for Buffers, a mixed EXEC reply with its failures as
  `:redis-cljs/warnings` metadata), `brpop!` / `blpop!` (blocking pops as a
  core.async channel feed) with `stop-feed!` and `feeds`, `end!` (graceful or
  forced), `status`, `last-error`, `on-end!`, `client?`, `api-version`.
- Pub/sub as core.async channels in `redis-cljs.pubsub`: `subscribe!`,
  `unsubscribe!`, `subscribed`, `publish!`; the protocol confirmations never reach
  a channel.
- Pools over node-redis's client pool in `redis-cljs.pool`: `pool!`, `connect!`,
  `command!`, `with-client!`, `status`, `last-error`, `end!`, and the
  `pool-` aliases.
- The shaping helpers of `redis-cljs.helpers`: `pairs->map`, `map->pairs`,
  `pop-reply`, `scan-reply`, `ok?`, `big-int`.
- The error taxonomy (`:redis-cljs/config-error`, `:redis-cljs/argument-error`,
  `:redis-cljs/network-error`, `:redis-cljs/api-error`), the explicit
  configuration translation tables and the environment reader of
  `redis-cljs.util`, and the single JS-interop namespace `redis-cljs.binding`.
- Project skeleton: deps.edn, package.json (`redis` 6.3.0 pinned), standalone
  shadow-cljs.edn (`node-test`, `dev`, `examples` builds), the publicity gate
  wired first in `run-tests.sh`, the scripted RESP2 fake server of the offline
  suite, the examples of the README run offline, the manual and the Redis API
  inventory under `docs/`.

[0.1.0]: https://github.com/borr3t/redis-cljs/releases/tag/v0.1.0
