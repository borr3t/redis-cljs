(ns redis-cljs.helpers-test
  "The shaping helpers: pure, no socket."
  (:require [cljs.test :refer [deftest is]]
            [redis-cljs.binding :as b]
            [redis-cljs.helpers :as helpers]))

(defn- thrown-data [f]
  (try (f) nil (catch :default e (ex-data e))))

(deftest pairs-and-maps
  (is (= {"name" "pencil" "price" "150"} (helpers/pairs->map ["name" "pencil" "price" "150"])))
  (is (= {} (helpers/pairs->map [])))
  (is (= {} (helpers/pairs->map nil)))
  (is (= ["name" "pencil" "price" 150] (helpers/map->pairs {"name" "pencil" "price" 150})))
  (is (= [] (helpers/map->pairs {})))
  (let [d (thrown-data #(helpers/pairs->map ["odd"]))]
    (is (= :redis-cljs/argument-error (:type d)))
    (is (= {:function 'redis-cljs.helpers/pairs->map :argument :pairs :received ["odd"]}
           (select-keys d [:function :argument :received]))))
  (is (= :m (:argument (thrown-data #(helpers/map->pairs {:name "x"})))) "keys must be strings")
  (is (= :m (:argument (thrown-data #(helpers/map->pairs ["a" "b"]))))))

(deftest pop-and-scan-replies
  (is (= {:key "q" :value "item"} (helpers/pop-reply ["q" "item"])))
  (is (nil? (helpers/pop-reply nil)))
  (is (= :reply (:argument (thrown-data #(helpers/pop-reply ["q"])))))
  (is (= {:cursor "17" :items ["a" "b"]} (helpers/scan-reply ["17" ["a" "b"]])))
  (is (= {:cursor "0" :items []} (helpers/scan-reply ["0" []])))
  (is (= :reply (:argument (thrown-data #(helpers/scan-reply ["0"]))))))

(deftest ok
  (is (true? (helpers/ok? "OK")))
  (is (false? (helpers/ok? nil)))
  (is (false? (helpers/ok? "PONG"))))

(deftest big-int-without-a-literal
  (let [n (helpers/big-int "9007199254740993")]
    (is (b/bigint? n))
    (is (= "9007199254740993" (.toString n))))
  (is (= "-5" (.toString (helpers/big-int "-5"))))
  (is (= "42" (.toString (helpers/big-int 42))))
  (doseq [bad ["1.5" 1.5 "x" "" nil "1e3"]]
    (let [d (thrown-data #(helpers/big-int bad))]
      (is (= :redis-cljs/argument-error (:type d)) (pr-str bad))
      (is (= :x (:argument d))))))
