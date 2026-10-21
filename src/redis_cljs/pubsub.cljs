(ns redis-cljs.pubsub
  "Pub/sub as core.async channels. A connection subscribes to a channel
   name with SUBSCRIBE; from then on the server pushes every PUBLISH on
   that name to it, at any time, until UNSUBSCRIBE or the connection ends.
   node-redis keeps the subscription bookkeeping, sends SUBSCRIBE when the
   first listener of a name is added and UNSUBSCRIBE when the last is
   removed, and hands a listener only real messages: the protocol's own
   `subscribe` / `unsubscribe` confirmations never reach it, and so never
   reach a channel of this namespace. subscribe! turns a listener into a
   core.async channel of {:channel :message} maps.

   Delivery: each channel has a sliding buffer (default-buffer-size, 1024
   maps, the :buffer-size option) filled the moment a message arrives,
   whether or not anyone is taking from it; when the buffer is full the
   OLDEST message is dropped to make room for the newest. Nothing is
   replayed: a message published while the connection was down, or
   dropped from a full buffer, is gone (that is Redis's own contract: a
   PUBLISH reaches the subscribers of the moment and is not queued).

   Which connection: under RESP2 a subscribed connection serves pub/sub
   only (the server refuses other commands on it with an error reply
   until the last UNSUBSCRIBE). Use a dedicated redis-cljs.core/connect!
   client for subscriptions and another one, or a pool, for commands and
   publish!."
  (:require [cljs.core.async :as async]
            [cljs.core.async.impl.protocols :as impl]
            [clojure.string :as str]
            [promesa.core :as p]
            [redis-cljs.binding :as b]
            [redis-cljs.core :as core]
            [redis-cljs.util :as util]))

(def default-buffer-size
  "Messages a channel holds before the oldest is dropped: the
   sliding-buffer size subscribe! uses when :buffer-size is absent."
  1024)

(def subscribe-options
  "Options of subscribe!, each with the shape of its value: :buffer-size,
   a positive integer (default default-buffer-size); :buffers?, true or
   false, true to receive :channel and :message as Buffers instead of
   strings."
  {:buffer-size #(and (integer? %) (pos? %))
   :buffers? boolean?})

(defn- channel-name? [s]
  (and (string? s) (not (str/blank? s))))

(defn- message-value?
  "A value PUBLISH can carry: a string, a Buffer or a number (sent as its
   decimal text)."
  [m]
  (or (string? m) (number? m) (b/buffer? m)))

;; ---------------------------------------------------------------------------
;; Subscription bookkeeping on the client handle's state atom:
;;   :subscriptions {channel-name [{:chan ch :listener f :buffers? b} ...]}
;;   :pubsub-end-hook? true  (one per client: closes everything on end)
;; ---------------------------------------------------------------------------

(defn- forget!
  "Drops one subscription from the client's bookkeeping and closes its
   channel."
  [client channel-name {:keys [chan]}]
  (async/close! chan)
  (swap! (:state client) update-in [:subscriptions channel-name]
         (fn [subs] (vec (remove #(identical? (:chan %) chan) subs)))))

(defn- close-all!
  "Every channel subscribed on the client: called once when the
   connection ends (no message can arrive any more)."
  [client]
  (doseq [[channel-name subs] (:subscriptions @(:state client)), sub subs]
    (forget! client channel-name sub)))

(defn- ensure-end-hook!
  [client]
  (when-not (:pubsub-end-hook? @(:state client))
    (swap! (:state client) assoc :pubsub-end-hook? true)
    (core/on-end! client (fn [] (close-all! client)))))

;; ---------------------------------------------------------------------------
;; Public surface
;; ---------------------------------------------------------------------------

(defn subscribe!
  "Subscribes the client's connection to `channel-name` and resolves with a
   core.async channel of its messages, one map per PUBLISH:
   {:channel \"news\" :message \"hello\"}: :channel the name as the server
   reports it, :message the published value verbatim (both Buffers with
   :buffers? true). Only messages for this name reach this channel;
   subscribe! again for another name, or again for the same name to get a
   second channel of the same messages. The promise resolves when the
   server has confirmed the SUBSCRIBE (the confirmation itself never
   appears on the channel). The channel has a sliding buffer of
   :buffer-size (default 1024): it fills whether or not a consumer takes,
   and the OLDEST message is dropped when it is full; nothing is replayed.
   The channel closes on unsubscribe! and when the connection ends (end!
   or a broken socket). Closing it yourself (cljs.core.async/close!) stops
   delivery at the next message: its listener is then detached, and when
   it was the last listener of the name node-redis sends UNSUBSCRIBE; use
   unsubscribe! to unsubscribe at once. Rejects with an argument-error for
   a non-client, a blank name or a bad option, with a
   :redis-cljs/api-error when the server refuses the SUBSCRIBE and a
   :redis-cljs/network-error when the connection is gone."
  ([client channel-name] (subscribe! client channel-name nil))
  ([client channel-name opts]
   (util/run-op
    (partial util/normalize-failure :subscribe b/error-info)
    (fn []
      (core/check-client! 'redis-cljs.pubsub/subscribe! client)
      (util/check! 'redis-cljs.pubsub/subscribe! :channel-name channel-name? "a non-blank string" channel-name)
      (util/check-options! 'redis-cljs.pubsub/subscribe! subscribe-options opts)
      (let [{:keys [buffer-size buffers?] :or {buffer-size default-buffer-size}} opts
            ch (async/chan (async/sliding-buffer buffer-size))
            sub (atom nil)
            listener (fn [message channel]
                       (if (impl/closed? ch)
                         (do (forget! client channel-name @sub)
                             (p/catch (b/unsubscribe (:js client) channel-name (:listener @sub) buffers?)
                                      (fn [_] nil)))
                         (async/put! ch {:channel channel :message message})))]
        (reset! sub {:chan ch :listener listener :buffers? (boolean buffers?)})
        (swap! (:state client) update-in [:subscriptions channel-name] (fnil conj []) @sub)
        (ensure-end-hook! client)
        (-> (b/subscribe (:js client) channel-name listener buffers?)
            (p/then (fn [_] ch))
            (p/catch (fn [e]
                       (forget! client channel-name @sub)
                       (throw e)))))))))

(defn unsubscribe!
  "Closes every channel subscribe! opened for `channel-name` on this
   client, removes their listeners, and resolves with nil once the server
   has confirmed the UNSUBSCRIBE that the removal of the last listener
   sends. The channels are closed before anything is sent, so nothing
   arrives on them after unsubscribe! is called. With no live channel for
   the name a plain UNSUBSCRIBE is still sent. Rejects with an
   argument-error for a non-client or a blank name, and as subscribe! does
   when the server refuses or the connection is gone."
  [client channel-name]
  (util/run-op
   (partial util/normalize-failure :unsubscribe b/error-info)
   (fn []
     (core/check-client! 'redis-cljs.pubsub/unsubscribe! client)
     (util/check! 'redis-cljs.pubsub/unsubscribe! :channel-name channel-name? "a non-blank string" channel-name)
     (let [subs (get-in @(:state client) [:subscriptions channel-name])]
       (doseq [sub subs] (forget! client channel-name sub))
       (-> (if (seq subs)
             (p/all (mapv (fn [{:keys [listener buffers?]}]
                            (b/unsubscribe (:js client) channel-name listener buffers?))
                          subs))
             (b/unsubscribe (:js client) channel-name nil nil))
           (p/then (fn [_] nil)))))))

(defn subscribed
  "The channel names this client currently has subscribe! channels open
   for, as a set (synchronous; a SUBSCRIBE sent through command! is not
   counted). Throws an argument-error for a non-client."
  [client]
  (core/check-client! 'redis-cljs.pubsub/subscribed client)
  (into #{} (keep (fn [[channel-name subs]] (when (seq subs) channel-name)))
        (:subscriptions @(:state client))))

(defn publish!
  "Sends PUBLISH `channel-name` `message` on the client (one that is not
   subscribed) and resolves with the number of subscribers that received
   it (an integer; 0 when nobody listens). `message` is a string or a
   Buffer (a number is sent as its decimal text), never encoded by the
   library. Rejects with an argument-error for a non-client, a blank name
   or a message that is not a string, Buffer or number, and as command!
   does otherwise."
  [client channel-name message]
  (util/run-op
   (partial util/normalize-failure :publish b/error-info)
   (fn []
     (core/check-client! 'redis-cljs.pubsub/publish! client)
     (util/check! 'redis-cljs.pubsub/publish! :channel-name channel-name? "a non-blank string" channel-name)
     (util/check! 'redis-cljs.pubsub/publish! :message message-value? "a string, a Buffer or a number" message)
     (core/command! client [:publish channel-name message]))))
