(ns redis-cljs.pubsub-test
  "Pub/sub channels against the fake server, which keeps the subscriptions
   of every connection, answers the SUBSCRIBE / UNSUBSCRIBE confirmations
   and pushes a PUBLISH to the connections subscribed to its channel, as a
   server does."
  (:require [cljs.test :refer [deftest is testing async]]
            [cljs.core.async :as async]
            [cljs.core.async.impl.protocols :as impl]
            [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as core]
            [redis-cljs.fake-server :as fake]
            [redis-cljs.pubsub :as pubsub]))

(defn- run [done f]
  (-> (f)
      (p/catch (fn [e] (is false (str "unexpected rejection: " (ex-message e) " " (pr-str (ex-data e))))))
      (p/finally (fn [_ _] (done)))))

(defn- failure [pr]
  (-> pr
      (p/then (fn [v] (is false (str "expected a rejection, got " (pr-str v))) nil))
      (p/catch (fn [e] (ex-data e)))))

(defn- thrown-data [f]
  (try (f) nil (catch :default e (ex-data e))))

(defn- with-server [script f]
  (p/let [server (fake/start! {:script script})]
    (p/let [outcome (-> (p/do (f server))
                        (p/then (fn [v] {:value v}))
                        (p/catch (fn [e] {:error e})))
            _ (fake/stop! server)]
      (if-let [e (:error outcome)]
        (throw e)
        (:value outcome)))))

(defn- take! [ch & [ms]]
  (p/create (fn [resolve _]
              (async/take! ch resolve)
              (js/setTimeout #(resolve ::timeout) (or ms 500)))))

(defn- drain! [ch]
  (loop [acc []]
    (if-some [v (async/poll! ch)]
      (recur (conj acc v))
      acc)))

(deftest messages-flow-between-two-connections
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [subscriber (core/connect! (fake/config server))
                    publisher (core/connect! (fake/config server))
                    news (pubsub/subscribe! subscriber "news")
                    subscribed (pubsub/subscribed subscriber)
                    heard-by (pubsub/publish! publisher "news" "hello")
                    nobody (pubsub/publish! publisher "other" "unheard")
                    first-message (take! news)
                    _ (pubsub/publish! publisher "news" 42)
                    second-message (take! news)
                    _ (pubsub/unsubscribe! subscriber "news")
                    after (pubsub/publish! publisher "news" "late")
                    _ (p/delay 20)
                    _ (core/end! subscriber)
                    _ (core/end! publisher)]
              (is (= #{"news"} subscribed))
              (is (= 1 heard-by) "publish! resolves with the receiver count")
              (is (= 0 nobody))
              (is (= {:channel "news" :message "hello"} first-message))
              (is (= {:channel "news" :message "42"} second-message) "a number is sent as its text")
              (is (= 0 after) "nobody listens after unsubscribe!")
              (is (impl/closed? news))
              (is (= [] (drain! news)) "the confirmations never reached the channel")
              (is (= #{} (pubsub/subscribed subscriber)))
              (is (= [["SUBSCRIBE" "news"] ["PUBLISH" "news" "hello"] ["PUBLISH" "other" "unheard"]
                      ["PUBLISH" "news" "42"] ["UNSUBSCRIBE" "news"] ["PUBLISH" "news" "late"]]
                     (fake/commands server))))))))))

(deftest a-second-channel-of-the-same-name-and-another-name
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [subscriber (core/connect! (fake/config server))
                    publisher (core/connect! (fake/config server))
                    a (pubsub/subscribe! subscriber "news")
                    b (pubsub/subscribe! subscriber "news")
                    other (pubsub/subscribe! subscriber "other")
                    _ (pubsub/publish! publisher "news" "x")
                    _ (pubsub/publish! publisher "other" "y")
                    _ (p/delay 30)
                    got [(drain! a) (drain! b) (drain! other)]
                    _ (pubsub/unsubscribe! subscriber "news")
                    still (pubsub/subscribed subscriber)
                    _ (core/end! subscriber)
                    _ (core/end! publisher)]
              (is (= [[{:channel "news" :message "x"}] [{:channel "news" :message "x"}] [{:channel "other" :message "y"}]]
                     got))
              (is (= #{"other"} still))
              (is (= 1 (count (filter #(= ["SUBSCRIBE" "news"] %) (fake/commands server))))
                  "one SUBSCRIBE per name, whatever the number of channels")
              (is (impl/closed? a))
              (is (impl/closed? b))
              (is (impl/closed? other) "ending the connection closes the rest"))))))))

(deftest buffers-and-the-buffer-size
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [subscriber (core/connect! (fake/config server))
                    publisher (core/connect! (fake/config server))
                    raw (pubsub/subscribe! subscriber "bin" {:buffers? true :buffer-size 2})
                    _ (pubsub/publish! publisher "bin" (js/Buffer.from #js [0 1]))
                    _ (pubsub/publish! publisher "bin" "2")
                    _ (pubsub/publish! publisher "bin" "3")
                    _ (p/delay 30)
                    got (drain! raw)
                    _ (core/end! subscriber)
                    _ (core/end! publisher)]
              (is (= 2 (count got)) "a sliding buffer of two dropped the oldest")
              (is (every? #(b/buffer? (:channel %)) got))
              (is (every? #(b/buffer? (:message %)) got))
              (is (= ["2" "3"] (mapv #(.toString (:message %)) got))))))))))

(deftest closing-the-channel-yourself-detaches-at-the-next-message
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [subscriber (core/connect! (fake/config server))
                    publisher (core/connect! (fake/config server))
                    ch (pubsub/subscribe! subscriber "news")
                    _ (async/close! ch)
                    _ (pubsub/publish! publisher "news" "x")
                    _ (p/delay 30)
                    gone (pubsub/subscribed subscriber)
                    _ (core/end! subscriber)
                    _ (core/end! publisher)]
              (is (= #{} gone))
              (is (= ["UNSUBSCRIBE" "news"] (last (fake/commands server))) "the last listener gone, UNSUBSCRIBE was sent"))))))))

(deftest a-refused-subscribe-rejects-and-cleans-up
  (async done
    (run done
      (fn []
        (with-server {"SUBSCRIBE" (fake/error "NOPERM this user has no permissions to access one of the channels used as arguments")}
          (fn [server]
            (p/let [subscriber (core/connect! (fake/config server))
                    d (failure (pubsub/subscribe! subscriber "secret"))
                    _ (core/end! subscriber)]
              (is (= :redis-cljs/api-error (:type d)))
              (is (= "NOPERM" (:code d)))
              (is (= #{} (pubsub/subscribed subscriber))))))))))

(deftest unsubscribe-without-a-channel-still-sends
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    r (pubsub/unsubscribe! client "never")
                    _ (core/end! client)]
              (is (nil? r))
              (is (= [["UNSUBSCRIBE" "never"]] (fake/commands server))))))))))

(deftest the-argument-contract
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [client (core/connect! (fake/config server))
                    rejected (fn [pr] (p/catch pr ex-data))
                    cases [[(rejected (pubsub/subscribe! 1 "news")) 'redis-cljs.pubsub/subscribe! :client]
                           [(rejected (pubsub/subscribe! client "")) 'redis-cljs.pubsub/subscribe! :channel-name]
                           [(rejected (pubsub/subscribe! client "news" {:size 1})) 'redis-cljs.pubsub/subscribe! :opts]
                           [(rejected (pubsub/subscribe! client "news" {:buffer-size 0})) 'redis-cljs.pubsub/subscribe! :buffer-size]
                           [(rejected (pubsub/unsubscribe! 1 "news")) 'redis-cljs.pubsub/unsubscribe! :client]
                           [(rejected (pubsub/unsubscribe! client nil)) 'redis-cljs.pubsub/unsubscribe! :channel-name]
                           [(rejected (pubsub/publish! 1 "news" "m")) 'redis-cljs.pubsub/publish! :client]
                           [(rejected (pubsub/publish! client " " "m")) 'redis-cljs.pubsub/publish! :channel-name]
                           [(rejected (pubsub/publish! client "news" {:a 1})) 'redis-cljs.pubsub/publish! :message]
                           [(rejected (pubsub/publish! client "news" nil)) 'redis-cljs.pubsub/publish! :message]
                           [(rejected (pubsub/publish! client "news" (fn [] "m"))) 'redis-cljs.pubsub/publish! :message]
                           [(rejected (pubsub/publish! client "news" :m)) 'redis-cljs.pubsub/publish! :message]
                           [(rejected (pubsub/subscribe! client "news" {:buffers? "true"})) 'redis-cljs.pubsub/subscribe! :buffers?]]
                    results (p/all (mapv first cases))
                    _ (core/end! client)]
              (doseq [[d [_ function argument]] (map vector results cases)]
                (is (= :redis-cljs/argument-error (:type d)) (str function " " argument))
                (is (= function (:function d)))
                (is (= argument (:argument d)))
                (is (string? (:expected d))))
              (let [d (thrown-data #(pubsub/subscribed 1))]
                (is (= 'redis-cljs.pubsub/subscribed (:function d)))
                (is (= :client (:argument d))))
              (is (= [] (fake/commands server)) "nothing was sent"))))))))
