(ns jepsen.mongodb.list-append
  "Elle list append workload"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr
                                  with-retry]]
            [elle.list-append :as elle.list-append]
            [jepsen [client :as client]
                    [checker :as checker]
                    [generator :as gen]
                    [util :as util :refer [timeout
                                           map-vals]]]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as list-append]
            [jepsen.mongodb [client :as c]]
            [slingshot.slingshot :as slingshot])
  (:import (java.util.concurrent TimeUnit)
           (com.mongodb MongoCommandException
                        MongoNodeIsRecoveringException
                        MongoNotPrimaryException
                        TransactionOptions
                        ReadConcern
                        ReadPreference
                        WriteConcern)
           (com.mongodb.client.model Filters
                                     UpdateOptions)))

(def db-name   "jepsendb")
(def coll-name "jepsencoll")

(defn txn-options
  "Constructs options for this transaction."
  [test txn]
  ; Transactions retry for well over 100 seconds and I cannot for the life of
  ; me find what switch makes that timeout shorter. MaxCommitTime only affects
  ; a *single* invocation of the transaction, not the retries. We work around
  ; this by timing out in Jepsen as well.
  (cond-> (TransactionOptions/builder)
    true (.maxCommitTime 5 TimeUnit/SECONDS)

    ; MongoDB *ignores* the DB and collection-level read and write concerns
    ; within a transaction, which seems... bad, because it actually
    ; *downgrades* safety if you chose high levels at the db or collection
    ; levels! We have to set them here too.
    (:txn-read-concern test)
    (.readConcern (c/read-concern (:txn-read-concern test)))

    (and (:txn-write-concern test)
         ; If the transaction is read-only, and we have
         ; no-read-only-txn-write-concern set, we don't bother setting the write
         ; concern.
         (not (and (every? (comp #{:r} first) txn)
                   (:no-read-only-txn-write-concern test))))
    (.writeConcern (c/write-concern (:txn-write-concern test) (:journal test)))

    ; Read preferences must always be primary; no sense in setting a pref here
    true .build))

(defn apply-mop!
  "Applies a transactional micro-operation to a connection."
  [test db session [f k v :as mop]]
  (let [coll (c/collection db coll-name)]
    ;(info (with-out-str
    ;        (println "db levels")
    ;        (prn :sn-rc ReadConcern/SNAPSHOT)
    ;        (prn :ma-rc ReadConcern/MAJORITY)
    ;        (prn :db-rc (.getReadConcern db))
    ;        (prn :ma-wc WriteConcern/MAJORITY)
    ;        (prn :db-wc (.getWriteConcern db))))
    (case f
      :r      [f k (vec (:value (c/find-one coll session k)))]
      :append (let [filt (Filters/eq "_id" k)
                    doc  (c/->doc {:$push {:value v}})
                    opts (.. (UpdateOptions.) (upsert true))
                    res  (if session
                           (.updateOne coll session filt doc opts)
                           (.updateOne coll filt doc opts))]
                ;(info :res res)
                mop))))

(defn create-coll!
  "Creates and optionally shards the collection for this test."
  [test conn]
  (with-retry [tries 5]
    (let [db (c/db conn db-name test)]
      (when (:sharded test)
        (c/admin-command! conn {:enableSharding db-name}))

      (info "creating collection")
      (let [coll (c/create-collection! db coll-name)]
        (info "Collection created")
        (when (:sharded test)
          ; Shard it!
          (c/admin-command! conn
                            {:shardCollection  (str db-name "." coll-name)
                             :key              {:_id :hashed}
                             ; WIP; gotta figure out how we're going to
                             ; generate queries with the shard key in them.
                             ;:key              {(case (:shard-key test)
                             ;                     :id :_id
                             ;                     :value :value)
                             ;                   :hashed}
                             :numInitialChunks 7})
          (info "Collection sharded"))))
    (catch MongoNotPrimaryException e
      ; sigh, why is this a thing
      (info "Ignoring MongoNotPrimaryException")
      nil)
    (catch MongoNodeIsRecoveringException e
      (info "Caught MongoNodeIsRecoveringException" tries (.getMessage e))
      (if (pos? tries)
        (do (info "Couldn't create collection:" (.getMessage e) " - retrying")
            (Thread/sleep 5000)
            (retry (dec tries)))
        (throw e)))
    (catch com.mongodb.MongoSocketReadTimeoutException e
      (if (pos? tries)
        (do (info "Timed out sharding DB and creating collection; waiting to retry")
            (Thread/sleep 5000)
            (retry (dec tries)))
        (throw e)))
    (catch MongoCommandException e
      (condp re-find (.getMessage e)
        #"Collection already exists" nil
        (throw e)))))

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node test)))

  (setup! [this test]
    (create-coll! test conn))

  (invoke! [this test op]
    (let [txn (:value op)]
      (c/with-errors op
        (timeout 5000 (assoc op :type :info, :error :timeout)
          (let [db   (c/db conn db-name test)
                txn' (if (and (<= (count txn) 1)
                              (not (:singleton-txns test)))
                       ; We can run without a transaction
                       [(apply-mop! test db nil (first txn))]

                       ; We need a transaction
                       (with-open [session (c/start-session conn)]
                         (let [opts (txn-options test txn)
                               body (c/txn
                                      ;(info :txn-begins)
                                      (mapv (partial apply-mop!
                                                     test db session)
                                            (:value op)))]
                           (.withTransaction session body opts))))]
            (assoc op :type :ok, :value txn'))))))

  (teardown! [this test])

  (close! [this test]
    (c/close! conn)))

(defn divergence-stats-checker
  "A checker which tries to estimate the fraction of writes which are lost to
  replica divergence.

  TODO: this is not very good. We use the longest value observed for a key as
  authoritative, but often Mongo loses a long value and replaces it with a
  shorter one, which causes us to undercount divergence. We could try to pick
  the *last* value observed, but of course that's not perfectly rigorous
  either..."
  []
  (reify checker/Checker
    (check [this test history opts]
      ; Build up a map of keys to the final observed values for those keys.sorted distinct observed values of that key.
      (let [sorted-values (->> history
                               (remove (comp #{:nemesis} :process))
                               elle.list-append/sorted-values)]
        (loopr [longest  (transient {})
                diverged (transient {})]
               [[k values] sorted-values]
               ; Find the longest value
               (let [longest-k (last values)]
                 (recur
                   (assoc! longest k longest-k)
                   ; Now zip through each value and record every value which
                   ; diverged from the longest version.
                   (loopr [diverged diverged]
                          [value values]
                          ; And for each element...
                          (recur
                            (loopr [i        0
                                    diverged diverged]
                                   [element value]
                                   (let [expected (nth longest-k i)]
                                     (recur (inc i)
                                            (if (= element expected)
                                              diverged
                                              (let [dk (-> diverged
                                                           (get k #{})
                                                           (conj element))]
                                                (assoc! diverged k dk)))))
                                   diverged)))))
               ; Great, now that we have the diverged values and longest values
               ; for k, compute stats
               (let [longest       (persistent! longest)
                     diverged      (persistent! diverged)
                     ; How many observed values in the longest values, across
                     ; all keys? Note that we're ignoring duplicates here.
                     longest-count (->> longest
                                        vals
                                        (map (comp count set))
                                        (reduce + 0))
                     ; How many divergent values?
                     div-count     (->> diverged
                                        vals
                                        (map count)
                                        (reduce + 0))]
                 {:valid?         (zero? div-count)
                  :longest-count  longest-count
                  :diverged-count div-count
                  :diverged-frac  (if (zero? (+ div-count longest-count))
                                    0
                                    (float (/ div-count
                                            (+ div-count longest-count))))
                  ;:longest        longest
                  ;:diverged       diverged
                  }))))))

(defn gen-48307
  "A generator variant which specifically targets
  https://jira.mongodb.org/browse/SERVER-48307. We produce single writes and a
  plethora of reads."
  [opts]
  (->> (list-append/gen {; We want a decent chance of choosing writes and reads
                         ; to *different* shards; the exponential approach will
                         ; concentrate most ops on one shard.
                         :key-dist       :uniform
                         :min-txn-length 4
                         :max-txn-length (:max-txn-length opts 4)
                         ; The default here, 3, wouldn't give us a decent
                         ; chance of picking keys from different shards.
                         :key-count      7
                         :max-writes-per-key (:max-writes-per-key opts)})
       ; Ensure every txn writes something
       (gen/filter (fn ensure-a-write [op]
                     (->> (:value op)
                          (map first)
                          (some #{:append}))))
       ; Ensure every txn writes to exactly one key
       (gen/map (fn rewrite [op]
                  (let [txn       (:value op)
                        ; Which key will we write?
                        write-key (->> txn
                                       (keep (fn [[f k v]]
                                               (when (= :append f)
                                                 k)))
                                       distinct
                                       rand-nth)
                        ; Rewrite other writes to reads
                        txn' (mapv (fn [[f k v :as mop]]
                                     (if (and (= f :append)
                                              (not= k write-key))
                                       [:r k nil]
                                       mop))
                                   txn)]
                    (assoc op :value txn'))))))

(defn workload
  "A generator, client, and checker for a list-append test."
  [opts]
  (-> (list-append/test {:key-count          10
                         :key-dist           :exponential
                         ;:key-dist           :uniform
                         :max-txn-length     (:max-txn-length opts 4)
                         :max-writes-per-key (:max-writes-per-key opts)
                         :consistency-models [:strong-snapshot-isolation]
                         :cycle-search-timeout 1000})
         (assoc :client (Client. nil))
         (update :checker (fn [c]
                            (checker/compose
                              {:elle c
                               :divergence (divergence-stats-checker)})))
         (cond->
           (:repro-48307 opts) (assoc :generator (gen-48307 opts)))))
