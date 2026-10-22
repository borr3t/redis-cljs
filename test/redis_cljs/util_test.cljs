(ns redis-cljs.util-test
  "The pure helpers: translation tables, configuration validation, commands
   as data, the environment reader's parsing, the error taxonomy and the
   promise adapter. No socket, no upstream object."
  (:require [cljs.test :refer [deftest is testing async]]
            [clojure.string :as str]
            [promesa.core :as p]
            [redis-cljs.util :as util]))

(defn- thrown-data [f]
  (try (f) nil (catch :default e (ex-data e))))

(defn- thrown-message [f]
  (try (f) nil (catch :default e (ex-message e))))

;; ---------------------------------------------------------------------------
;; Configuration
;; ---------------------------------------------------------------------------

(deftest config-forms
  (is (= {} (util/connect-config nil)))
  (is (= {:host "h"} (util/connect-config {:host "h"})))
  (testing "a bare URL string is not a configuration"
    (let [d (thrown-data #(util/connect-config "redis://h:1"))]
      (is (= :redis-cljs/config-error (:type d)))
      (is (= {:function 'redis-cljs.core/connect! :argument :config :received "redis://h:1"}
             (select-keys d [:function :argument :received])))
      (is (str/includes? (:expected d) ":url"))))
  (let [d (thrown-data #(util/connect-config 42))]
    (is (= :redis-cljs/config-error (:type d)))
    (is (= {:function 'redis-cljs.core/connect! :argument :config :received 42}
           (select-keys d [:function :argument :received])))
    (is (string? (:expected d)))))

(deftest normalize-fills-the-defaults-and-drops-nils
  (is (= {:reconnect false :host "h"} (util/normalize-config {:host "h" :port nil})))
  (is (= {:reconnect 100} (util/normalize-config {:reconnect 100}))))

(deftest check-config-refuses-before-anything-is-sent
  (testing "a valid map passes through"
    (let [cfg (util/normalize-config {:host "h" :port 6380 :database 2 :password "p" :tls true
                                      :tls-options {:reject-unauthorized false}
                                      :connect-timeout-ms 100 :reconnect (fn [_ _] 10)})]
      (is (= cfg (util/check-config! cfg)))))
  (testing "an unknown key"
    (let [d (thrown-data #(util/check-config! (util/normalize-config {:host "h" :timeout 5})))]
      (is (= :redis-cljs/config-error (:type d)))
      (is (= #{:timeout} (:unknown d)))
      (is (= (set (keys util/client-config-keys)) (:allowed d)))
      (is (= :config (:argument d)))
      (is (= 'redis-cljs.core/connect! (:function d)))))
  (testing "neither :url nor :host"
    (let [d (thrown-data #(util/check-config! (util/normalize-config {:port 6379})))]
      (is (= :redis-cljs/config-error (:type d)))
      (is (str/includes? (:expected d) ":url or :host"))))
  (testing "a wrong value names the key and the expectation, never the value"
    (let [d (thrown-data #(util/check-config! (util/normalize-config {:host "h" :port "6379"})))
          m (thrown-message #(util/check-config! (util/normalize-config {:host "h" :port "6379"})))]
      (is (= :redis-cljs/config-error (:type d)))
      (is (= {:argument :port :expected "an integer between 1 and 65535" :received "6379"}
             (select-keys d [:argument :expected :received])))
      (is (= "redis-cljs: redis-cljs.core/connect!: port must be an integer between 1 and 65535" m)))
    (doseq [[config argument] [[{:host ""} :host]
                               [{:host "h" :port 0} :port]
                               [{:host "h" :port 70000} :port]
                               [{:host "h" :database -1} :database]
                               [{:host "h" :database 1.5} :database]
                               [{:host "h" :name " "} :name]
                               [{:host "h" :reconnect "yes"} :reconnect]
                               [{:host "h" :reconnect -5} :reconnect]
                               [{:host "h" :connect-timeout-ms -1} :connect-timeout-ms]
                               [{:host "h" :tls "yes"} :tls]
                               [{:url "http://h"} :url]
                               [{:url "redis://"} :url]
                               [{:url "redis://h x"} :url]
                               [{:host "h" :maint-notifications "auto"} :config]]]
      (let [d (thrown-data #(util/check-config! (util/normalize-config config)))]
        (is (= :redis-cljs/config-error (:type d)) (pr-str config))
        (is (= argument (:argument d)) (pr-str config)))))
  (testing "a :tls-options key outside the table"
    (let [d (thrown-data #(util/check-config! (util/normalize-config {:host "h" :tls-options {:verify false}})))]
      (is (= #{:verify} (:unknown d)))
      (is (= :tls-options (:argument d)))))
  (testing "not a map"
    (is (= :redis-cljs/config-error (:type (thrown-data #(util/check-config! "redis://h")))))))

(deftest config-errors-carry-no-secret
  (testing "a wrong password value is not in the message nor the data"
    (let [config (util/normalize-config {:host "h" :password {:token "SYNTHETIC_SECRET"}})
          d (thrown-data #(util/check-config! config))
          m (thrown-message #(util/check-config! config))]
      (is (= :password (:argument d)))
      (is (= "***" (:received d)))
      (is (not (str/includes? (pr-str d) "SYNTHETIC_SECRET")))
      (is (not (str/includes? m "SYNTHETIC_SECRET")))))
  (testing "a whole configuration carried in an error is redacted"
    (doseq [config [{:host "h" :password "SYNTHETIC_SECRET" :timeout 5}
                    {:url {:password "SYNTHETIC_SECRET"}}
                    {:host "h" :tls-options "SYNTHETIC_SECRET"}
                    {:url "redis://u:one@SYNTHETIC_SECRET@h:1" :port "x"}
                    {:url "redis://u:SYNTHETIC_SECRET@h:notaport"}
                    {:url "redis://u:SYNTHETIC_SECRET@h" :tls true}
                    {:password "SYNTHETIC_SECRET"}
                    {:host "h" :password "SYNTHETIC_SECRET" :tls-options {:key "SYNTHETIC_SECRET" :passphrase "SYNTHETIC_SECRET" :verify 1}}
                    {:url "redis://u:SYNTHETIC_SECRET@h x"}
                    {:url "redis://u:SYNTHETIC_SECRET@h:1" :port "x"}]]
      (let [d (thrown-data #(util/check-config! (util/normalize-config config)))
            m (thrown-message #(util/check-config! (util/normalize-config config)))]
        (is (= :redis-cljs/config-error (:type d)) (pr-str config))
        (is (not (str/includes? (pr-str d) "SYNTHETIC_SECRET")) (pr-str config))
        (is (not (str/includes? m "SYNTHETIC_SECRET")) (pr-str config)))))
  (testing "a non-map configuration holding a secret is redacted too"
    (is (not (str/includes? (pr-str (thrown-data #(util/connect-config "redis://u:SYNTHETIC_SECRET@h")))
                            "SYNTHETIC_SECRET"))))
  (testing "a password written with a raw ?, # or / splits the URL: it is refused and nothing after the scheme is carried"
    (doseq [config [{:url "redis://u:SYNTHETIC?SECRET@h"}
                    {:url "redis://u:SYNTHETIC#SECRET@h"}
                    {:url "redis://u:SYNTHETIC/SECRET@h"}
                    {:url "rediss://u:SYNTHETIC?SE@CRET@h:6380/0"}]]
      (let [d (thrown-data #(util/check-config! (util/normalize-config config)))
            m (thrown-message #(util/check-config! (util/normalize-config config)))]
        (is (= :url (:argument d)) (pr-str config))
        (is (= (str (subs (:url config) 0 (+ 3 (str/index-of (:url config) "://"))) "***") (:received d)) (pr-str config))
        (is (not (str/includes? (pr-str d) "SYNTHETIC")) (pr-str config))
        (is (not (str/includes? m "SYNTHETIC")) (pr-str config))))
    (let [d (thrown-data #(util/connect-config "u:SYNTHETIC?SECRET@h"))]
      (is (= "***" (:received d)) "no scheme, nothing to show")
      (is (not (str/includes? (pr-str d) "SYNTHETIC"))))))

(deftest urls-are-parsed-and-checked-like-the-explicit-keys
  (testing "parse-url"
    (is (= {:tls? false :host "h"} (util/parse-url "redis://h")))
    (is (= {:tls? true :username "u" :password "p" :host "h" :port 6380 :database 2}
           (util/parse-url "rediss://u:p@h:6380/2?x=1")))
    (is (= {:tls? false :username "u" :password "one@SECRET" :host "h"} (util/parse-url "redis://u:one@SECRET@h")))
    (is (= {:tls? false :username "" :password "p" :host "h"} (util/parse-url "redis://:p@h")))
    (is (= {:tls? false :host "[::1]" :port 6379} (util/parse-url "redis://[::1]:6379")))
    (is (= {:tls? false :host "h" :port 0} (util/parse-url "redis://h:0")))
    (testing "an empty port or path leaves the part unspecified, as upstream does"
      (is (= {:tls? false :host "h"} (util/parse-url "redis://h/")))
      (is (= {:tls? false :host "h"} (util/parse-url "redis://h:")))
      (is (= {:tls? false :host "h"} (util/parse-url "redis://h:/")))
      (is (= {:tls? false :host "[::1]"} (util/parse-url "redis://[::1]:/"))))
    (testing "the credentials are percent-decoded, as upstream reads them"
      (is (= {:tls? false :username "u@name" :password "p@ss:word" :host "h" :database 2}
             (util/parse-url "redis://u%40name:p%40ss%3Aword@h/2")))
      (is (= {:tls? false :username "u@name" :password "SECRET" :host "h"} (util/parse-url "redis://u@name:SECRET@h")))
      (is (nil? (util/parse-url "redis://u%zz:p@h")) "a malformed escape, which upstream refuses"))
    (testing "a database too long for a number parses to Infinity, for check-config! to refuse"
      (is (infinite? (:database (util/parse-url (str "redis://h/" (apply str (repeat 400 "9"))))))))
    (doseq [bad ["redis:///0" "redis://" "redis://h:notaport" "redis://h/-1" "redis://h/1.5" "http://h" "redis://h x" nil 7]]
      (is (nil? (util/parse-url bad)) (pr-str bad))))
  (testing "the effective configuration is refused like the explicit keys"
    (doseq [[config argument] [[{:url "redis:///0"} :url]
                               [{:url "redis://h:0/0"} :port]
                               [{:url "redis://h:70000"} :port]
                               [{:url "redis://h/-1"} :url]
                               [{:url "redis://h/1.5"} :url]
                               [{:url "redis://h:notaport"} :url]
                               [{:url "redis://h" :tls true} :tls]
                               [{:url "rediss://h" :tls false} :tls]
                               [{:url (str "redis://h/" (apply str (repeat 400 "9")))} :database]
                               [{:host "h" :database js/Infinity} :database]
                               [{:url "redis://u%zz:p@h"} :url]]]
      (let [d (thrown-data #(util/check-config! (util/normalize-config config)))]
        (is (= :redis-cljs/config-error (:type d)) (pr-str config))
        (is (= argument (:argument d)) (pr-str config)))))
  (testing "a consistent :tls passes"
    (is (map? (util/check-config! (util/normalize-config {:url "rediss://h" :tls true}))))
    (is (map? (util/check-config! (util/normalize-config {:url "redis://h:6380/3"})))))
  (testing "an empty port or path passes, the part unspecified"
    (is (map? (util/check-config! (util/normalize-config {:url "redis://h/"}))))
    (is (map? (util/check-config! (util/normalize-config {:url "redis://h:"}))))
    (is (map? (util/check-pool-config! (util/normalize-config {:url "redis://h:/"}))))))

(deftest check-pool-config-adds-the-pool-keys
  (let [cfg (util/normalize-config {:url "redis://h" :minimum 2 :maximum 4 :acquire-timeout-ms 10})]
    (is (= cfg (util/check-pool-config! cfg))))
  (let [d (thrown-data #(util/check-pool-config! (util/normalize-config {:host "h" :max 3})))]
    (is (= 'redis-cljs.pool/pool! (:function d)))
    (is (= #{:max} (:unknown d))))
  (is (= :maximum (:argument (thrown-data #(util/check-pool-config! (util/normalize-config {:host "h" :maximum "4"}))))))
  (is (= :minimum (:argument (thrown-data #(util/check-pool-config! (util/normalize-config {:host "h" :minimum 0}))))))
  (testing ":minimum above :maximum"
    (let [d (thrown-data #(util/check-pool-config! (util/normalize-config {:host "h" :minimum 3 :maximum 1})))]
      (is (= :maximum (:argument d)))
      (is (= 1 (:received d))))
    (is (= :maximum (:argument (thrown-data #(util/check-pool-config! (util/normalize-config {:host "h" :minimum 101})))))
        "above the default maximum of 100"))
  (is (= :redis-cljs/config-error (:type (thrown-data #(util/check-config! (util/normalize-config {:host "h" :maximum 4})))))
      "a pool key is not a client key"))

(deftest config-translation
  (testing "the socket options are nested, RESP 2 and no maintenance notifications are fixed, reconnect false stays"
    (is (= {"RESP" 2
            "maintNotifications" "disabled"
            "socket" {"host" "h" "port" 6380 "connectTimeout" 100 "reconnectStrategy" false
                      "tls" true "rejectUnauthorized" false}
            "username" "u" "password" "p" "database" 2 "name" "n"
            "pingInterval" 1000 "disableOfflineQueue" true}
           (util/config->upstream (util/normalize-config
                                   {:host "h" :port 6380 :connect-timeout-ms 100 :tls true
                                    :tls-options {:reject-unauthorized false}
                                    :username "u" :password "p" :database 2 :name "n"
                                    :ping-interval-ms 1000 :disable-offline-queue true})))))
  (testing ":reconnect true leaves the strategy to upstream"
    (is (= {"RESP" 2 "maintNotifications" "disabled" "url" "redis://h"}
           (util/config->upstream (util/normalize-config {:url "redis://h" :reconnect true})))))
  (testing "a delay and a function pass through"
    (is (= 250 (get-in (util/config->upstream (util/normalize-config {:host "h" :reconnect 250})) ["socket" "reconnectStrategy"])))
    (let [f (fn [_ _] 1)]
      (is (identical? f (get-in (util/config->upstream (util/normalize-config {:host "h" :reconnect f})) ["socket" "reconnectStrategy"])))))
  (testing "an unknown key, a wrong value or a bad :tls-options is refused, not dropped"
    (doseq [config [{:host "h" :timeout 5} {:host 7} {:host "h" :tls-options 7}
                    {:host "h" :tls-options {:verify 1}} "redis://h" nil]]
      (let [d (thrown-data #(util/config->upstream config))]
        (is (= :redis-cljs/argument-error (:type d)) (pr-str config))
        (is (= 'redis-cljs.util/config->upstream (:function d)) (pr-str config))))
    (is (not (str/includes? (pr-str (thrown-data #(util/config->upstream {:host "h" :tls-options {:key "SYNTHETIC_SECRET" :verify 1}})))
                            "SYNTHETIC_SECRET"))))
  (testing "the check runs on the configuration itself: a secret of the wrong shape and a pool key out of range are refused, the error redacted"
    (doseq [config [{:host "h" :password 7}
                    {:url "redis://u:SYNTHETIC SECRET@h"}
                    {:url "redis://u:SYNTHETIC?SECRET@h"}
                    {:url "redis://u:SYNTHETIC#SECRET@h"}
                    {:host "h" :minimum 0}
                    {:host "h" :minimum 3 :maximum 1}
                    {:host "h" :acquire-timeout-ms -1}]]
      (let [d (thrown-data #(util/config->upstream config))]
        (is (= :redis-cljs/argument-error (:type d)) (pr-str config))
        (is (= 'redis-cljs.util/config->upstream (:function d)) (pr-str config))
        (is (not (str/includes? (pr-str d) "SYNTHETIC")) (pr-str config))
        (is (not (str/includes? (pr-str d) " 7")) (pr-str config))))
    (is (= "***" (:password (:received (thrown-data #(util/config->upstream {:host "h" :password 7}))))))
    (is (= "redis://u:***@h" (:url (:received (thrown-data #(util/config->upstream {:url "redis://u:SYNTHETIC SECRET@h"}))))))
    (is (= "redis://***" (:url (:received (thrown-data #(util/config->upstream {:url "redis://u:SYNTHETIC?SECRET@h"}))))))
    (is (= "redis://***" (:url (:received (thrown-data #(util/config->upstream {:url "redis://u:SYNTHETIC#SECRET@h"})))))))
  (testing "the pool split"
    (is (= [{"RESP" 2 "maintNotifications" "disabled" "socket" {"host" "h" "reconnectStrategy" false}}
            {"minimum" 2 "maximum" 4}]
           (util/pool-config->upstream (util/normalize-config {:host "h" :minimum 2 :maximum 4}))))))

(deftest redaction-hides-every-secret
  (is (= {:host "h" :password "***"} (util/redacted {:host "h" :password "p"})))
  (testing "a password holding @ is masked whole, up to the last @"
    (is (= {:url "redis://u:***@h:1"} (util/redacted {:url "redis://u:one@SECRET@h:1"})))
    (is (= "redis://u:***@h" (util/redacted "redis://u:one@two@SECRET@h"))))
  (testing "a username holding @ does not move the mask: the first colon of the credentials starts it"
    (is (= {:url "redis://u@name:***@h"} (util/redacted {:url "redis://u@name:SECRET@h"})))
    (is (= "redis://u@name:***@h/2" (util/redacted "redis://u@name:SE:CR@ET@h/2")))
    (is (= "redis://:***@h" (util/redacted "redis://:SECRET@h")))
    (is (= "redis://user@h" (util/redacted "redis://user@h")) "a username alone is no secret")
    (is (= "redis://h:6379/0" (util/redacted "redis://h:6379/0")) "no credentials, nothing masked"))
  (testing "an @ past the authority (after a /, ? or #) leaves the credentials' end unknown: everything after the scheme is masked"
    (is (= {:url "redis://***"} (util/redacted {:url "redis://u:SYNTHETIC?SECRET@h"})))
    (is (= "redis://***" (util/redacted "redis://u:SYNTHETIC#SECRET@h")))
    (is (= "redis://***" (util/redacted "redis://u:SYNTHETIC/SECRET@h")))
    (is (= "rediss://***" (util/redacted "rediss://u:SYN@THETIC?SECRET@h")) "an @ in the authority too does not settle it")
    (is (= "redis://***" (util/redacted "redis://h/0?x=a@b")) "a valid URL with @ in its query is masked the same way")
    (is (= "***" (util/redacted "u:SYNTHETIC?SECRET@h")) "no scheme, nothing kept")
    (is (= "redis://u:***@h/0?x=1" (util/redacted "redis://u:SECRET@h/0?x=1")) "an @ in the authority alone is read as before")
    (is (= "redis://h:notaport" (util/redacted "redis://h:notaport")) "no @, no credentials"))
  (testing "a :url or :tls-options of the wrong type is masked whole"
    (is (= {:url "***"} (util/redacted {:url {:password "SECRET"}})))
    (is (= {:host "h" :tls-options "***"} (util/redacted {:host "h" :tls-options "PRIVATE_KEY"}))))
  (is (= {:url "redis://u:***@h:1/2"} (util/redacted {:url "redis://u:verysecret@h:1/2"})))
  (is (= {:url "redis://h:1"} (util/redacted {:url "redis://h:1"})))
  (is (= {:host "h" :tls-options {:ca "public" :key "***" :passphrase "***"}}
         (util/redacted {:host "h" :tls-options {:ca "public" :key "PRIVATE" :passphrase "secret"}})))
  (is (= "not a map" (util/redacted "not a map"))))

;; ---------------------------------------------------------------------------
;; Environment
;; ---------------------------------------------------------------------------

(deftest unwrap-env-value-strips-one-layer-of-quotes
  (is (= "s3cret-" (util/unwrap-env-value "\"s3cret-\"")))
  (is (= " secret " (util/unwrap-env-value "\" secret \"")) "what the quotes held is kept as it is")
  (is (= "" (util/unwrap-env-value "\"\"")) "a quoted empty string is the empty string")
  (is (= "" (util/unwrap-env-value "''")))
  (is (= "x" (util/unwrap-env-value " 'x' ")))
  (is (= "x" (util/unwrap-env-value "`x`")))
  (is (= "-x-" (util/unwrap-env-value "-x-")) "dashes are part of the value")
  (is (= "-secret-" (util/unwrap-env-value "\"-secret-\"")))
  (is (= "\"x\"" (util/unwrap-env-value "\"\"x\"\"")) "only one layer")
  (is (= "a\"" (util/unwrap-env-value "a\"")) "an unmatched quote stays")
  (is (= "'a\"" (util/unwrap-env-value "'a\"")) "quotes of different kinds stay")
  (is (nil? (util/unwrap-env-value "   ")))
  (is (nil? (util/unwrap-env-value nil)))
  (is (nil? (util/unwrap-env-value 7))))

(deftest parse-integer-is-pure-and-strict
  (is (= 0 (util/parse-integer "0")))
  (is (= 6380 (util/parse-integer "6380")))
  (is (= 9007199254740991 (util/parse-integer "9007199254740991")))
  (doseq [bad ["" "-1" "1.5" "1e3" " 1" "x" nil 7]]
    (is (nil? (util/parse-integer bad)) (pr-str bad))))

(deftest env-to-config
  (is (= {} (util/env->config {})))
  (is (= {:url "redis://h:1" :host "h" :port 6380 :username "u" :password "p" :database 3 :tls true}
         (util/env->config {"REDIS_URL" "\"redis://h:1\"" "REDIS_HOST" "h" "REDIS_PORT" "'6380'"
                            "REDIS_USERNAME" "u" "REDIS_PASSWORD" "p" "REDIS_DATABASE" "3"
                            "REDIS_TLS" "Yes" "OTHER" "ignored"})))
  (doseq [[spelling value] {"0" false "false" false "no" false "off" false "OFF" false
                            "1" true "true" true "yes" true "on" true "TRUE" true}]
    (is (= {:tls value} (util/env->config {"REDIS_TLS" spelling})) spelling))
  (testing "a REDIS_TLS value that is none of the spellings fails loud"
    (let [d (thrown-data #(util/env->config {"REDIS_TLS" "flase"}))]
      (is (= :redis-cljs/config-error (:type d)))
      (is (= :tls (:argument d)))
      (is (= "flase" (:received d)))))
  (is (= {} (util/env->config {"REDIS_HOST" "  "})) "a blank variable is absent")
  (is (= {:password ""} (util/env->config {"REDIS_PASSWORD" "\"\""})) "a quoted empty string is a value")
  (is (= :port (:argument (thrown-data #(util/env->config {"REDIS_PORT" "\"\""})))))
  (let [d (thrown-data #(util/env->config {"REDIS_PORT" "six"}))]
    (is (= :redis-cljs/config-error (:type d)))
    (is (= {:function 'redis-cljs.core/from-env :argument :port :received "six"}
           (select-keys d [:function :argument :received]))))
  (is (= :database (:argument (thrown-data #(util/env->config {"REDIS_DATABASE" "-1"})))))
  (testing "a bad password-bearing variable never reaches an error"
    (let [d (thrown-data #(util/env->config {"REDIS_PORT" "six" "REDIS_PASSWORD" "SYNTHETIC_SECRET"}))]
      (is (not (str/includes? (pr-str d) "SYNTHETIC_SECRET"))))))

;; ---------------------------------------------------------------------------
;; Commands as data
;; ---------------------------------------------------------------------------

(deftest commands-translate-to-upstream-arguments
  (is (= ["SET" "k" "v"] (util/command->upstream [:set "k" "v"])))
  (is (= ["SET" "k" "v" "EX" "60" "NX"] (util/command->upstream [:set "k" "v" :ex 60 :nx])))
  (is (= ["HSET" "h" "f" "v"] (util/command->upstream ["hset" "h" "f" "v"])))
  (is (= ["CLIENT" "ID"] (util/command->upstream [:client-id])))
  (is (= ["CLIENT" "ID"] (util/command->upstream ["CLIENT ID"])))
  (is (= ["BRPOP" "q" "0.5"] (util/command->upstream [:brpop "q" 0.5])))
  (is (= ["GET" "k"] (util/command->upstream '(:get "k"))) "a list is a command too")
  (testing "a value stays the string it is: no reinterpretation"
    (is (= ["SET" "n" "42"] (util/command->upstream [:set "n" "42"])))
    (is (= ["SET" "j" "{\"a\":1}"] (util/command->upstream [:set "j" "{\"a\":1}"]))))
  (testing "an object passes only when the wire test accepts it"
    (let [o (js-obj)]
      (is (identical? o (last (util/command->upstream 'f [:set "k" o] (fn [x] (identical? x o))))))
      (is (= :redis-cljs/argument-error (:type (thrown-data #(util/command->upstream 'f [:set "k" o])))))
      (is (= :redis-cljs/argument-error (:type (thrown-data #(util/command->upstream 'f [:set "k" (fn [] 1)] (constantly true)))))
          "a function is refused before the wire test"))))

(deftest commands-are-checked
  (let [d (thrown-data #(util/check-command! "SET k v"))
        m (thrown-message #(util/check-command! "SET k v"))]
    (is (= :redis-cljs/argument-error (:type d)))
    (is (= {:function 'redis-cljs.core/command! :argument :command :received "SET k v"}
           (select-keys d [:function :argument :received])))
    (is (= "redis-cljs: redis-cljs.core/command!: command must be a non-empty vector [:cmd \"arg\" ...]" m)))
  (is (= :redis-cljs/argument-error (:type (thrown-data #(util/check-command! [])))))
  (is (= 0 (:index (thrown-data #(util/check-command! [42 "k"])))))
  (is (= 0 (:index (thrown-data #(util/check-command! ["" "k"])))) "a blank name")
  (is (= 0 (:index (thrown-data #(util/check-command! ["  " "k"])))))
  (testing "a name blank once normalized"
    (doseq [head [(keyword "") (symbol "") :- :-- "" " "]]
      (let [d (thrown-data #(util/command->upstream [head "k"]))]
        (is (= :redis-cljs/argument-error (:type d)) (pr-str head))
        (is (= 0 (:index d)) (pr-str head)))))
  (is (= [:get "k"] (util/check-command! [:get "k"])))
  (testing "nil, booleans, collections, symbols and functions are refused with their index"
    (doseq [[cmd index] [[[:set "k" nil] 2] [[:set "k" true] 2] [[:rpush "q" ["a"]] 2] [[:set "k" {:a 1}] 2]
                         [[:set 'k "v"] 1] [[:set "k" (fn [] 1)] 2]]]
      (let [d (thrown-data #(util/command->upstream cmd))]
        (is (= :redis-cljs/argument-error (:type d)) (pr-str cmd))
        (is (= index (:index d)) (pr-str cmd))))))

(deftest command-options-are-checked
  (is (nil? (util/check-command-options! nil)))
  (is (= {:buffers? true} (util/check-command-options! {:buffers? true})))
  (is (= {:buffers? false} (util/check-command-options! {:buffers? false})))
  (let [d (thrown-data #(util/check-command-options! {:as :buffer}))]
    (is (= :redis-cljs/argument-error (:type d)))
    (is (= #{:as} (:unknown d)))
    (is (= :opts (:argument d))))
  (testing "a value of the wrong type names the option"
    (let [d (thrown-data #(util/check-command-options! {:buffers? "false"}))]
      (is (= :redis-cljs/argument-error (:type d)))
      (is (= :buffers? (:argument d)))
      (is (= "true or false" (:expected d)))
      (is (= "false" (:received d)))))
  (is (= :redis-cljs/argument-error (:type (thrown-data #(util/check-command-options! [:buffers?]))))))

(deftest keys-argument-forms
  (is (= ["q"] (util/keys-argument 'f "q")))
  (is (= ["a" "b"] (util/keys-argument 'f ["a" "b"])))
  (doseq [bad ["" [] ["a" ""] [:a] 7 nil]]
    (is (= :keys (:argument (thrown-data #(util/keys-argument 'f bad)))) (pr-str bad))))

;; ---------------------------------------------------------------------------
;; Errors
;; ---------------------------------------------------------------------------

(deftest the-taxonomy
  (is (= #{:redis-cljs/config-error :redis-cljs/argument-error :redis-cljs/network-error :redis-cljs/api-error}
         util/error-types))
  (let [e (util/argument-error 'redis-cljs.core/command! :client "a client handle" 7)]
    (is (= "redis-cljs: redis-cljs.core/command!: client must be a client handle" (ex-message e)))
    (is (= {:type :redis-cljs/argument-error :function 'redis-cljs.core/command! :argument :client
            :expected "a client handle" :received 7}
           (dissoc (ex-data e) :message)))
    (is (= :redis-cljs/argument-error (util/error-type e)))
    (is (nil? (util/error-type (ex-info "other" {:type :x/y}))))
    (is (nil? (util/error-type (js/Error. "plain"))))))

(deftest error-reply-codes
  (is (= "WRONGTYPE" (util/error-reply-code "WRONGTYPE Operation against a key holding the wrong kind of value")))
  (is (= "ERR" (util/error-reply-code "ERR unknown command 'FOO'")))
  (is (= "NOAUTH" (util/error-reply-code "NOAUTH Authentication required."))))

(deftest failures-normalize-by-kind
  (let [info-of (fn [e] {:message (.-message e) :error-reply? (= "reply" (.-name e)) :code (.-code e)})
        reply (doto (js/Error. "WRONGTYPE wrong") (aset "name" "reply"))
        socket (doto (js/Error. "connect ECONNREFUSED 127.0.0.1:1") (aset "code" "ECONNREFUSED"))
        passthrough (ex-info "mine" {:type :mine})]
    (let [d (ex-data (util/normalize-failure :command info-of reply {:command [:get "k"]}))]
      (is (= :redis-cljs/api-error (:type d)))
      (is (= "WRONGTYPE wrong" (:message d)))
      (is (= "WRONGTYPE" (:code d)))
      (is (= [:get "k"] (:command d)))
      (is (= :command (:op d)))
      (is (identical? reply (:cause d))))
    (let [d (ex-data (util/normalize-failure :connect info-of socket))]
      (is (= :redis-cljs/network-error (:type d)))
      (is (= "ECONNREFUSED" (:code d))))
    (is (identical? passthrough (util/normalize-failure :command info-of passthrough)))))

(deftest warnings-of-mixed-replies
  (let [error? #(and (map? %) (:error-reply? %))]
    (is (nil? (util/warnings-of error? ["OK" 1])))
    (is (nil? (util/warnings-of error? "OK")))
    (is (= [{:index 1 :code "WRONGTYPE" :message "WRONGTYPE wrong"}]
           (util/warnings-of error? ["OK" {:error-reply? true :message "WRONGTYPE wrong"} 2])))))

(deftest run-op-settles-every-outcome
  (async done
    (p/let [sync-value (util/run-op identity (fn [] 1))
            async-value (util/run-op identity (fn [] (p/resolved 2)))
            thrown (p/catch (util/run-op (fn [e] (ex-info "wrapped" {:was (ex-message e)}))
                                         (fn [] (throw (js/Error. "boom"))))
                            ex-data)
            rejected (p/catch (util/run-op (fn [e] (ex-info "wrapped" {:was (ex-message e)}))
                                           (fn [] (p/rejected (js/Error. "later"))))
                              ex-data)]
      (is (= 1 sync-value))
      (is (= 2 async-value))
      (is (= {:was "boom"} thrown))
      (is (= {:was "later"} rejected))
      (is (not (identical? (util/run-op identity (fn [] nil)) (util/run-op identity (fn [] nil))))
          "every call builds its own promise")
      (done))))
