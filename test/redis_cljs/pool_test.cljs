(ns redis-cljs.pool-test
  "The pool surface against the fake server: the REAL node-redis client
   pool opens real clients to the scripted server, so connect!, command!,
   with-client!, the counters and end! run end to end with no Redis
   server and no network beyond the loopback interface."
  (:require [cljs.test :refer [deftest is testing async]]
            [cljs.core.async :as async]
            [clojure.string :as str]
            [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as core]
            [redis-cljs.fake-server :as fake]
            [redis-cljs.pool :as pool]))

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

(defn- counts [pool]
  (select-keys (pool/status pool) [:total-clients :idle-clients :waiting-tasks :open?]))

(deftest a-pool-is-created-without-connecting
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (let [pool (pool/pool! (fake/config server {:minimum 1 :maximum 2 :password "s3cret"}))]
              (is (pool/pool? pool))
              (is (not (pool/pool? {:js nil})))
              (is (= {:total-clients 0 :idle-clients 0 :waiting-tasks 0 :open? false :closing? false
                      :minimum 1 :maximum 2}
                     (pool/status pool)))
              (is (nil? (pool/last-error pool)))
              (let [shown (pr-str pool)]
                (is (str/starts-with? shown "#redis-cljs/pool{"))
                (is (str/includes? shown ":maximum 2"))
                (is (str/includes? shown ":status :closed"))
                (is (not (str/includes? shown "s3cret"))))
              (p/let [_ (p/delay 20)]
                (is (= 0 (fake/connections server)))))))))))

(deftest a-bad-pool-configuration-is-a-config-error
  (let [d (thrown-data #(pool/pool! {:host "127.0.0.1" :max 3}))]
    (is (= :redis-cljs/config-error (:type d)))
    (is (= 'redis-cljs.pool/pool! (:function d)))
    (is (= #{:max} (:unknown d))))
  (is (str/includes? (:expected (thrown-data #(pool/pool! {:maximum 3}))) ":url or :host"))
  (is (= :minimum (:argument (thrown-data #(pool/pool! {:host "h" :minimum "1"})))))
  (is (= :maximum (:argument (thrown-data #(pool/pool! {:host "h" :minimum 3 :maximum 1})))) ":minimum above :maximum")
  (is (= "***" (:received (thrown-data #(pool/pool! {:host "h" :password {:token "SYNTHETIC_SECRET"}})))))
  (testing "a URL the pool's clients could not use is refused at pool!, redacted"
    (doseq [[config argument] [[{:url "redis://u:SYNTHETIC_SECRET@h" :tls true} :tls]
                               [{:url "redis://u:SYNTHETIC_SECRET@h:notaport"} :url]
                               [{:url "redis:///0"} :url]]]
      (let [d (thrown-data #(pool/pool! config))
            m (try (pool/pool! config) nil (catch :default e (ex-message e)))]
        (is (= :redis-cljs/config-error (:type d)) (pr-str config))
        (is (= argument (:argument d)) (pr-str config))
        (is (not (str/includes? (pr-str d) "SYNTHETIC_SECRET")) (pr-str config))
        (is (not (str/includes? m "SYNTHETIC_SECRET")) (pr-str config)))))
  (is (= :redis-cljs/config-error (:type (thrown-data #(pool/pool! 42))))))

(deftest connect-command-and-with-client
  (async done
    (run done
      (fn []
        (with-server {"GET" "v" "SET" (fake/simple "OK") "BRPOP" (fake/in-order ["q" "job"] nil)}
          (fn [server]
            (p/let [pool (pool/pool! (fake/config server {:minimum 1 :maximum 2 :name "pooled"}))
                    connected (pool/connect! pool)
                    after-connect (counts pool)
                    v (pool/command! pool [:get "k"])
                    lent (pool/with-client! pool
                           (fn [client]
                             (p/let [ok (core/command! client [:set "k" "v"])
                                     popped (take! (core/brpop! client "q" {:timeout-s 1}))
                                     _ (take! (core/brpop! client "q" {:timeout-s 1}))]
                               {:client? (core/client? client)
                                :status (core/status client)
                                :during (counts pool)
                                :ok ok
                                :popped popped
                                :shown (pr-str client)})))
                    plain (pool/with-client! pool (fn [_] :plain-value))
                    after (counts pool)
                    ended (pool/end! pool)
                    again (pool/end! pool)]
              (is (identical? pool connected))
              (is (= {:total-clients 1 :idle-clients 1 :waiting-tasks 0 :open? true} after-connect))
              (is (= "v" v))
              (is (:client? lent))
              (is (= :ready (:status lent)))
              (is (= {:total-clients 1 :idle-clients 0 :waiting-tasks 0 :open? true} (:during lent)))
              (is (= "OK" (:ok lent)))
              (is (= {:key "q" :value "job"} (:popped lent)))
              (is (str/includes? (:shown lent) ":name \"pooled\""))
              (is (= :plain-value plain) "a plain value is resolved too")
              (is (= {:total-clients 1 :idle-clients 1 :waiting-tasks 0 :open? true} after))
              (is (nil? ended))
              (is (nil? again))
              (is (= false (:open? (pool/status pool))))
              (is (= [["CLIENT" "SETNAME" "pooled"] ["GET" "k"] ["SET" "k" "v"]
                      ["BRPOP" "q" "1"] ["BRPOP" "q" "1"] ["BRPOP" "q" "1"]]
                     (fake/commands server))
                  "the first BRPOP feed popped twice (the second pop timed out), the second once"))))))))

(deftest a-loan-attaches-no-listener-of-its-own
  (async done
    (run done
      (fn []
        (with-server {"GET" "v"}
          (fn [server]
            (p/let [pool (pool/pool! (fake/config server {:maximum 1}))
                    _ (pool/connect! pool)
                    counts (fn [client] [(b/listener-count (:js client) "error") (b/listener-count (:js client) "end")])
                    first-loan (pool/with-client! pool (fn [client] (counts client)))
                    handles (p/all (repeatedly 12 #(pool/with-client! pool (fn [client] client))))
                    last-loan (pool/with-client! pool (fn [client] (counts client)))
                    _ (pool/end! pool)]
              (is (= first-loan last-loan) "thirteen loans, the same listeners")
              (is (= 1 (count (set handles))) "one handle per upstream client"))))))))

(deftest a-lent-client-is-not-ended-by-the-caller
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [pool (pool/pool! (fake/config server))
                    _ (pool/connect! pool)
                    d (failure (pool/with-client! pool (fn [client] (core/end! client))))
                    _ (pool/end! pool)]
              (is (= :redis-cljs/argument-error (:type d)))
              (is (= 'redis-cljs.core/end! (:function d)))
              (is (= :client (:argument d))))))))))

(deftest with-client-returns-the-client-on-failure-and-rethrows
  (async done
    (run done
      (fn []
        (with-server {"GET" (fake/error "ERR no")}
          (fn [server]
            (p/let [pool (pool/pool! (fake/config server {:maximum 1}))
                    _ (pool/connect! pool)
                    thrown (failure (pool/with-client! pool (fn [_] (throw (ex-info "mine" {:type :mine})))))
                    rejected (failure (pool/with-client! pool (fn [client] (core/command! client [:get "k"]))))
                    after (counts pool)
                    _ (pool/end! pool)]
              (is (= :mine (:type thrown)) "an ex-info of caller code passes through")
              (is (= :redis-cljs/api-error (:type rejected)))
              (is (= 1 (:idle-clients after)) "the client went back to the pool each time"))))))))

(deftest the-pool-rejects-when-every-client-is-busy-for-too-long
  (async done
    (run done
      (fn []
        (with-server {"GET" (fake/delayed 150 "slow")}
          (fn [server]
            (p/let [pool (pool/pool! (fake/config server {:maximum 1 :acquire-timeout-ms 30}))
                    _ (pool/connect! pool)
                    [slow d] (let [slow (pool/command! pool [:get "k"])]
                               (p/all [slow (failure (pool/command! pool [:get "k"]))]))
                    _ (pool/end! pool)]
              (is (= "slow" slow))
              (is (= :redis-cljs/network-error (:type d)))
              (is (str/includes? (:message d) "Timeout waiting for a client")))))))))

(deftest a-refused-connection-rejects-the-pool
  (async done
    (run done
      (fn []
        (p/let [port (fake/closed-port)
                pool (pool/pool! {:host "127.0.0.1" :port port})
                d (failure (pool/connect! pool))]
          (is (= :redis-cljs/network-error (:type d)))
          (is (= "ECONNREFUSED" (:code d))))))))

(deftest after-end-every-use-rejects
  (async done
    (run done
      (fn []
        (with-server {"GET" "v"}
          (fn [server]
            (p/let [pool (pool/pool! (fake/config server))
                    _ (pool/connect! pool)
                    _ (pool/end! pool)
                    command (failure (pool/command! pool [:get "k"]))
                    lend (failure (pool/with-client! pool (fn [_] nil)))]
              (is (= :redis-cljs/network-error (:type command)))
              (is (= "The client is closed" (:message command)))
              (is (= :redis-cljs/network-error (:type lend))))))))))

(deftest the-aliases-are-the-functions
  (is (identical? pool/connect! pool/pool-connect!))
  (is (identical? pool/command! pool/pool-command!))
  (is (identical? pool/end! pool/pool-end!))
  (is (identical? pool/status pool/pool-status)))

(deftest the-argument-contract
  (async done
    (run done
      (fn []
        (with-server {}
          (fn [server]
            (p/let [pool (pool/pool! (fake/config server))
                    rejected (fn [pr] (p/catch pr ex-data))
                    cases [[(rejected (pool/connect! 1)) 'redis-cljs.pool/connect! :pool]
                           [(rejected (pool/command! 1 [:get "k"])) 'redis-cljs.pool/command! :pool]
                           [(rejected (pool/command! pool "GET k")) 'redis-cljs.pool/command! :command]
                           [(rejected (pool/command! pool [:get "k"] {:x 1})) 'redis-cljs.pool/command! :opts]
                           [(rejected (pool/with-client! 1 identity)) 'redis-cljs.pool/with-client! :pool]
                           [(rejected (pool/with-client! pool 7)) 'redis-cljs.pool/with-client! :f]
                           [(rejected (pool/end! 1)) 'redis-cljs.pool/end! :pool]]
                    results (p/all (mapv first cases))]
              (doseq [[d [_ function argument]] (map vector results cases)]
                (is (= :redis-cljs/argument-error (:type d)) (str function " " argument))
                (is (= function (:function d)))
                (is (= argument (:argument d)))
                (is (string? (:expected d))))
              (is (= :pool (:argument (thrown-data #(pool/status 1)))))
              (is (= :pool (:argument (thrown-data #(pool/last-error 1)))))
              (is (= [] (fake/commands server)) "nothing was sent"))))))))
