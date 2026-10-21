(ns redis-cljs.helpers
  "A few shaping helpers for replies and arguments. They move values
   between the flat vectors RESP2 uses and the maps a program prefers, and
   never touch the values themselves: a field stays the string the server
   sent, a value stays the string or Buffer it is. Pure, except big-int,
   which builds a JavaScript BigInt through the binding namespace."
  (:require [redis-cljs.binding :as b]
            [redis-cljs.util :as util]))

(defn pairs->map
  "A flat vector of alternating fields and values, as HGETALL, HRANDFIELD
   WITHVALUES or CONFIG GET answer under RESP2, -> a map of field -> value:
   [\"name\" \"pencil\" \"price\" \"150\"] -> {\"name\" \"pencil\" \"price\"
   \"150\"}. Keys stay strings (Buffers with :buffers?), values stay what
   they are. nil -> {}. Throws an argument-error for anything but a
   vector of even length."
  [pairs]
  (util/check! 'redis-cljs.helpers/pairs->map :pairs
               #(or (nil? %) (and (sequential? %) (even? (count %))))
               "a vector of alternating fields and values (even length) or nil" pairs)
  (into {} (map vec) (partition 2 pairs)))

(defn map->pairs
  "A map of field -> value -> the flat vector of arguments HSET, HMSET or
   MSET take after the key: {\"name\" \"pencil\" \"price\" \"150\"} ->
   [\"name\" \"pencil\" \"price\" \"150\"], so (into [:hset \"product:1\"]
   (map->pairs m)) is the command. Fields must be strings; values are
   passed as they are (strings, Buffers, numbers; command! checks them).
   Throws an argument-error for a non-map or a non-string field."
  [m]
  (util/check! 'redis-cljs.helpers/map->pairs :m
               #(and (map? %) (every? string? (keys %)))
               "a map whose keys are strings" m)
  (into [] cat m))

(defn pop-reply
  "The reply of BRPOP / BLPOP / BRPOPLPUSH-style commands run through
   command!, as the map brpop! delivers: [\"queue\" \"item\"] -> {:key
   \"queue\" :value \"item\"}; nil (the timeout) -> nil. Throws an
   argument-error for anything but a two-element vector or nil."
  [reply]
  (util/check! 'redis-cljs.helpers/pop-reply :reply
               #(or (nil? %) (and (sequential? %) (= 2 (count %))))
               "a vector of [key value] or nil" reply)
  (when reply
    {:key (first reply) :value (second reply)}))

(defn scan-reply
  "The reply of SCAN, SSCAN, HSCAN or ZSCAN run through command!:
   [\"17\" [\"k1\" \"k2\"]] -> {:cursor \"17\" :items [\"k1\" \"k2\"]}. The
   cursor stays a string (\"0\" means the scan is complete); the items are
   what the server sent (HSCAN and ZSCAN interleave fields or members with
   values or scores: pairs->map shapes them). Throws an argument-error for
   anything but a two-element vector."
  [reply]
  (util/check! 'redis-cljs.helpers/scan-reply :reply
               #(and (sequential? %) (= 2 (count %)) (sequential? (second %)))
               "a vector of [cursor items]" reply)
  {:cursor (first reply) :items (vec (second reply))})

(defn ok?
  "True when `reply` is the simple string \"OK\" that SET, HMSET, SELECT,
   AUTH and the like answer; false for anything else (including nil, the
   reply of a SET NX that did not set)."
  [reply]
  (= "OK" reply))

(defn big-int
  "A JavaScript BigInt for a 64-bit integer that a JavaScript number
   cannot hold exactly, to pass as a command argument (command! sends it
   as its decimal text) or to parse an integer reply's string form from a
   value stored as text: (big-int \"9007199254740993\"). Accepts a string
   of decimal digits with an optional leading minus, or an integer number.
   Throws an argument-error for anything else (a fractional number, a
   string with other characters); no BigInt literal is involved."
  [x]
  (util/check! 'redis-cljs.helpers/big-int :x
               #(or (and (string? %) (re-matches #"-?[0-9]+" %))
                    (and (number? %) (integer? %)))
               "a string of decimal digits (optional leading minus) or an integer" x)
  (b/->bigint (if (number? x) (str x) x)))
