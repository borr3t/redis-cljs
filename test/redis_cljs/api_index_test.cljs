(ns redis-cljs.api-index-test
  "The API index is checked against the source in both directions:
   - every public var of every public namespace has an entry in the API
     reference of docs/redis-cljs.org (a heading `**** namespace/name`),
     and is named in the Public API index of README.md;
   - every such heading of the manual names a public var that exists.
   The public vars are read from the compiler's analysis at compile time
   (redis-cljs.doctest/public-vars), so a function added to the source
   without its reference fails here."
  (:require ["fs" :as fs]
            [cljs.test :refer [deftest is testing]]
            [clojure.string :as str]
            [redis-cljs.core]
            [redis-cljs.helpers]
            [redis-cljs.pool]
            [redis-cljs.pubsub]
            [redis-cljs.util])
  (:require-macros [redis-cljs.doctest :refer [public-vars]]))

(def ^:private indexed
  "The public namespaces and their public vars. redis-cljs.binding is
   internal (its vars are public to the compiler, not part of the API)
   and is not indexed."
  {"redis-cljs.core" (public-vars 'redis-cljs.core)
   "redis-cljs.pool" (public-vars 'redis-cljs.pool)
   "redis-cljs.pubsub" (public-vars 'redis-cljs.pubsub)
   "redis-cljs.helpers" (public-vars 'redis-cljs.helpers)
   "redis-cljs.util" (public-vars 'redis-cljs.util)})

(def ^:private record-vars
  "The record types and the constructor functions a defrecord emits
   (Client, ->Client, map->Client): public by the compiler's account, not
   part of the API."
  #(re-matches #"(->|map->)?[A-Z].*" %))

(defn- slurp [path]
  (fs/readFileSync path "utf8"))

(defn- manual-entries
  "Every `**** namespace/name` heading of the manual, as a set of
   namespace/name strings."
  []
  (into #{} (map second) (re-seq #"(?m)^\*\*\*\* (redis-cljs\.[a-z-]+/\S+)" (slurp "docs/redis-cljs.org"))))

(deftest every-public-var-is-documented
  (let [entries (manual-entries)
        readme (slurp "README.md")]
    (is (seq entries) "the manual has an API reference")
    (doseq [[ns names] indexed, name names
            :when (not (record-vars name))
            :let [full (str ns "/" name)]]
      (testing full
        (is (contains? entries full) (str "no reference entry in docs/redis-cljs.org for " full))
        (is (re-find (re-pattern (str "[`(]" (str/replace name #"[.*+?^${}()|\[\]\\]" "\\$&") "[`\\s)]")) readme)
            (str "README.md does not index " full))))))

(deftest every-documented-entry-exists
  (let [known (into #{} (for [[ns names] indexed, name names] (str ns "/" name)))]
    (doseq [entry (manual-entries)]
      (testing entry
        (is (contains? known entry) (str "the manual documents " entry ", which the source does not have"))))))
