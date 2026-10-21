(ns redis-cljs.dev
  "The Node runtime a REPL attaches to (shadow-cljs build :dev):

     npx shadow-cljs watch dev          ; terminal 1
     node target/dev.js                 ; terminal 2
     npx shadow-cljs cljs-repl dev      ; terminal 3

   It starts a fake server (redis-cljs.fake-server) and keeps the process
   alive; from the REPL, (redis-cljs.core/connect! (redis-cljs.fake-server/config @server))
   opens a client against it. No Redis server, no network beyond the
   loopback interface."
  (:require [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as core]
            [redis-cljs.fake-server :as fake]
            [redis-cljs.helpers :as helpers]
            [redis-cljs.pool :as pool]
            [redis-cljs.pubsub :as pubsub]
            [redis-cljs.util :as util]))

(defonce server (atom nil))

(defn main [& _]
  (p/let [s (fake/start! {:script {"GET" "value"}})]
    (reset! server s)
    (println "fake server on port" (fake/port s) "; api-version" core/api-version)
    (b/keep-process-alive!)))
