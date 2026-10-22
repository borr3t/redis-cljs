(ns redis-cljs.examples-test
  "The examples of the README, run. Network-free beyond the loopback
   interface: the server is the scripted RESP2 fake of
   redis-cljs.fake-server, started by dev/redis_cljs/examples.cljs, and a
   tripwire stands where a configuration without a fake would open a
   socket.

   The README is held to the code, exactly:
   - what stands between a README:begin and a README:end mark of
     dev/redis_cljs/examples.cljs stands in README.md character for
     character;
   - what `main` prints for every example (the result, pretty-printed)
     and for every one-liner (the form, then `;; =>` and its value)
     stands in README.md character for character;
   - every ```clojure block of README.md is one of those, the namespace
     form of the examples, or the deps.edn snippet of the Requirements:
     a snippet the examples do not produce fails here."
  (:require ["fs" :as fs]
            [cljs.test :refer [deftest is testing async use-fixtures]]
            [clojure.string :as str]
            [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as redis]
            [redis-cljs.examples :as ex]
            [redis-cljs.pool :as pool]))

;; ---------------------------------------------------------------------------
;; No socket is opened here but to a fake
;; ---------------------------------------------------------------------------

(def ^:private config->js-before (atom nil))

(use-fixtures :once
  {:before (fn []
             (reset! config->js-before b/config->js)
             (ex/no-server!))
   :after (fn []
            (set! b/config->js @config->js-before))})

;; ---------------------------------------------------------------------------
;; Reading the README
;; ---------------------------------------------------------------------------

(defn- slurp [path]
  (fs/readFileSync path "utf8"))

(defn- readme [] (slurp "README.md"))

(defn- region
  "The text between the marks README:begin `label` and README:end `label`
   of the examples file, nil when either mark is missing."
  [label]
  (let [[_ found] (re-find (re-pattern (str "(?s);; README:begin " label "\\n(.*?)\\n\\s*;; README:end " label))
                           (slurp "dev/redis_cljs/examples.cljs"))]
    found))

(defn- shows?
  "Does README.md contain `text` exactly?"
  [text]
  (str/includes? (readme) text))

(defn- printed-in-readme
  "Asserts README.md shows what `main` prints for `title` and `value`."
  [title value]
  (let [text (ex/printed title value)]
    (is (shows? text) (str "README.md does not show, exactly:\n" text))))

(defn- clojure-blocks
  "Every ```clojure fenced block of README.md, as its text."
  []
  (map second (re-seq #"(?s)```clojure\n(.*?)```" (readme))))

(defn- run
  "Runs the promise of `(f)` as an async test body."
  [done f]
  (-> (f)
      (p/catch (fn [e] (is false (str "unexpected rejection: " (ex-message e) " " (pr-str (ex-data e))))))
      (p/finally (fn [_ _] (done)))))

(def ^:private region-labels
  ["config" "quick-start" "queue" "pubsub" "pool" "errors"])

(def ^:private requirements-snippet
  ";; deps.edn of the consuming app, next to a checkout:\n{:deps {io.github.borr3t/redis-cljs {:local/root \"../redis-cljs\"}}}\n")

(def ^:private ns-form
  "The namespace form of the examples without its docstring: (ns ...)
   with the :require and :require-macros clauses."
  (let [source (slurp "dev/redis_cljs/examples.cljs")
        clauses (re-find #"(?s)\(:require .*?\]\)\)" source)]
    (str "(ns redis-cljs.examples\n  " clauses "\n")))

;; ---------------------------------------------------------------------------
;; Tests
;; ---------------------------------------------------------------------------

(deftest a-forgotten-fake-fails-at-once
  (async done
    (run done
      (fn []
        (-> (redis/connect! ex/config)
            (p/then (fn [_] (is false "a connection was opened")))
            (p/catch (fn [e]
                       (is (= :redis-cljs.examples/offline (:type (ex-data e))))
                       (is (= 6379 (:port (ex-data e)))))))))))

(deftest a-forgotten-fake-fails-a-pool-at-once
  (is (= :redis-cljs.examples/offline
         (:type (ex-data (try (pool/pool! ex/config) (catch :default e e)))))))

(deftest the-quick-start-answers-as-shown
  (async done
    (run done
      (fn []
        (p/let [r (ex/quick-start-offline)]
          (is (= {:views 1
                  :product {"name" "pencil" "price" "150"}
                  :missing nil
                  :sent [["CLIENT" "SETNAME" "redis-cljs-examples"]
                         ["HSET" "product:1" "name" "pencil" "price" "150"]
                         ["INCR" "product:1:views"]
                         ["HGETALL" "product:1"]
                         ["GET" "product:9"]]}
                 r))
          (printed-in-readme "quick start" r))))))

(deftest the-url-form-selects-its-database
  (async done
    (run done
      (fn []
        (p/let [r (ex/url-offline)]
          (is (= {:pong "PONG"
                  :sent [["CLIENT" "SETNAME" "redis-cljs-examples"] ["SELECT" "2"] ["PING"]]}
                 r))
          (printed-in-readme "from a URL" r))))))

(deftest the-work-queue-pops-first-in-first-out-then-stops
  (async done
    (run done
      (fn []
        (p/let [r (ex/work-queue-offline)]
          (is (= {:queued 2
                  :popped [{:key "jobs" :value "invoice-41"} {:key "jobs" :value "invoice-42"}]
                  :then nil
                  :sent [["CLIENT" "SETNAME" "redis-cljs-examples"]
                         ["CLIENT" "SETNAME" "redis-cljs-examples"]
                         ["RPUSH" "jobs" "invoice-41" "invoice-42"]
                         ["BLPOP" "jobs" "1"] ["BLPOP" "jobs" "1"] ["BLPOP" "jobs" "1"]]}
                 r))
          (printed-in-readme "a work queue over BLPOP" r))))))

(deftest a-publish-on-one-connection-arrives-on-the-other
  (async done
    (run done
      (fn []
        (p/let [r (ex/announce-offline)]
          (is (= {:receivers 1
                  :message {:channel "news" :message "price drop on pencils"}
                  :still-subscribed #{}
                  :sent [["CLIENT" "SETNAME" "redis-cljs-examples"]
                         ["CLIENT" "SETNAME" "redis-cljs-examples"]
                         ["SUBSCRIBE" "news"]
                         ["PUBLISH" "news" "price drop on pencils"]
                         ["UNSUBSCRIBE" "news"]]}
                 r))
          (printed-in-readme "pub/sub across two connections" r))))))

(deftest the-pool-serves-commands-and-lends-a-client
  (async done
    (run done
      (fn []
        (p/let [r (ex/pooled-offline)]
          (is (= {:ttl 86400
                  :popped [{:key "jobs" :value "invoice-41"} {:key "jobs" :value "invoice-42"} nil]
                  :status {:total-clients 1 :idle-clients 1 :maximum 2}
                  :sent [["CLIENT" "SETNAME" "redis-cljs-examples"]
                         ["TTL" "product:1"]
                         ["BLPOP" "jobs" "1"]
                         ["BLPOP" "jobs" "1"]
                         ["BLPOP" "jobs" "1"]]}
                 r))
          (printed-in-readme "a pool" r))))))

(deftest failures-are-data
  (async done
    (run done
      (fn []
        (p/let [r (ex/errors-offline)]
          (is (= {:wrong-type {:ok? false :why :server
                               :error {:code "WRONGTYPE"
                                       :message "WRONGTYPE Operation against a key holding the wrong kind of value"
                                       :command [:hget "product:1:views" "name"]}}
                  :not-vector {:ok? false :why :argument
                               :error {:function 'redis-cljs.core/command! :argument :command}}
                  :unknown-key {:ok? false :why :configuration
                                :error {:argument :config :unknown #{:timeout}}}
                  :refused {:ok? false :why :connection :error {:code "ECONNREFUSED"}}
                  :after-end {:ok? false :why :connection
                              :error {:message "The client is closed"}}}
                 r))
          (printed-in-readme "failures are data" r))))))

(deftest an-unknown-failure-is-rethrown
  (async done
    (run done
      (fn []
        (-> (ex/outcome #(throw (ex-info "other" {:type :other/error})))
            (p/then (fn [_] (is false "a foreign error was swallowed")))
            (p/catch (fn [e] (is (= :other/error (:type (ex-data e)))))))))))

(deftest the-one-liners-hold
  (let [value (into {} ex/one-liners)]
    (is (= (count ex/one-liners) (count value)) "no one-liner is shown twice")
    (is (= ["SET" "k" "v" "EX" "60" "NX"] (value "(util/command->upstream [:set \"k\" \"v\" :ex 60 :nx])")))
    (is (= {"name" "pencil" "price" "150"} (value "(helpers/pairs->map [\"name\" \"pencil\" \"price\" \"150\"])")))
    (is (= ["name" "pencil" "price" "150"] (value "(helpers/map->pairs {\"name\" \"pencil\", \"price\" \"150\"})")))
    (is (= {:key "jobs" :value "invoice-41"} (value "(helpers/pop-reply [\"jobs\" \"invoice-41\"])")))
    (is (= {:cursor "17" :items ["product:1" "product:2"]} (value "(helpers/scan-reply [\"17\" [\"product:1\" \"product:2\"]])")))
    (is (true? (value "(helpers/ok? \"OK\")")))
    (is (= {"RESP" 2 "maintNotifications" "disabled"
            "socket" {"reconnectStrategy" false "host" "127.0.0.1" "port" 6379} "database" 2}
           (value "(util/config->upstream (util/normalize-config {:host \"127.0.0.1\", :port 6379, :database 2}))")))
    (is (= {:host "cache.example.test" :password "***"} (value "(util/redacted {:host \"cache.example.test\", :password \"hunter2\"})")))
    (is (= "s3cret-" (value "(util/unwrap-env-value \"\\\"s3cret-\\\"\")")))
    (is (= [:redis-cljs/api-error :redis-cljs/argument-error :redis-cljs/config-error :redis-cljs/network-error]
           (value "(sort util/error-types)")))
    (is (false? (value "(redis/client? config)")))
    (is (= "0.1.0" (value "redis/api-version")))
    (is (= 1024 (value "pubsub/default-buffer-size")))))

(deftest the-readme-shows-what-runs-exactly
  (testing "the marked code of the examples, character for character"
    (doseq [label region-labels
            :let [code (region label)]]
      (is (some? code) (str "no region " label))
      (is (shows? (str code "\n")) (str "README.md does not show " label " exactly:\n" code))))
  (testing "the one-liners with their values, as main prints them"
    (is (shows? (ex/printed-one-liners))))
  (testing "the namespace form of the examples"
    (is (shows? ns-form)))
  (testing "how the examples are run, and where the manual is"
    (is (shows? "npx shadow-cljs compile examples && node target/examples.js"))
    (is (shows? "docs/redis-cljs.org"))
    (is (shows? "docs/redis-api-inventory.org"))
    (is (shows? "\"redis\": \"6.3.0\""))))

(deftest the-readme-shows-nothing-else
  (async done
    (run done
      (fn []
        (p/let [results [["quick start" (ex/quick-start-offline)]
                         ["from a URL" (ex/url-offline)]
                         ["a work queue over BLPOP" (ex/work-queue-offline)]
                         ["pub/sub across two connections" (ex/announce-offline)]
                         ["a pool" (ex/pooled-offline)]
                         ["failures are data" (ex/errors-offline)]]
                results (p/all (map (fn [[title pr]] (p/then pr (fn [r] [title r]))) results))
                allowed (-> #{requirements-snippet ns-form (ex/printed-one-liners)}
                            (into (map (fn [label] (str (region label) "\n"))) region-labels)
                            ;; a printed result stands with its title line
                            (into (map (fn [[title r]] (ex/printed title r))) results))
                blocks (clojure-blocks)]
          (is (seq blocks))
          (doseq [block blocks]
            (is (contains? allowed block) (str "README.md shows a clojure block the examples do not produce:\n" block))))))))
