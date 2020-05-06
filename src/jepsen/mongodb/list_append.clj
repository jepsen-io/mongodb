(ns jepsen.mongodb.list-append
  "Elle list append workload"
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [client :as client]
                    [checker :as checker]
                    [util :as util :refer [timeout]]]
            [jepsen.generator.pure :as gen]
            [jepsen.tests.cycle :as cycle]
            [jepsen.tests.cycle.append :as list-append]
            [jepsen.mongodb [client :as c]]
            [slingshot.slingshot :as slingshot])
  (:import (java.util.concurrent TimeUnit)
           (com.mongodb TransactionOptions
                        ReadConcern
                        ReadPreference
                        WriteConcern)
           (com.mongodb.client.model Filters
                                     UpdateOptions)))

(def db-name   "jepsendb")
(def coll-name "jepsencoll")

(defn txn-options
  "Constructs options for this transaction."
  [test]
  ; Transactions retry for well over 100 seconds and I cannot for the life of
  ; me find what switch makes that timeout shorter. MaxCommitTime only affects
  ; a *single* invocation of the transaction, not the retries. We work around
  ; this by timing out in Jepsen as well.
  (cond-> (TransactionOptions/builder)
    true                  (.maxCommitTime 5 TimeUnit/SECONDS)
    ; MongoDB *ignores* the DB and collection-level read and write concerns
    ; within a transaction, which seems... bad, because it actually
    ; *downgrades* safety if you chose high levels at the db or collection
    ; levels! We have to set them here too.
    (:read-concern test)  (.readConcern (c/read-concern (:read-concern test)))
    (:write-concern test) (.writeConcern
                            (c/write-concern (:write-concern test)))
    true                  .build))


(defn apply-mop!
  "Applies a transactional micro-operation to a connection."
  [conn session [f k v :as mop]]
  (let [coll (-> conn
                 (c/db db-name test)
                 (c/collection coll-name))]
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

(defrecord Client [conn]
  client/Client
  (open! [this test node]
    (assoc this :conn (c/open node c/mongos-port)))

  (setup! [this test]
    ; Collections have to be predeclared; transactions can't create them.
    (try (let [db   (c/db conn db-name test)]
           ; Shard database
           (c/admin-command! conn {:enableSharding db-name})
           (let [coll (c/create-collection! db coll-name)]
             (info "Collection created")
             ; Shard it!
             (c/admin-command! conn
                               {:shardCollection  (str db-name "." coll-name)
                                :key              {:_id :hashed}
                                :numInitialChunks 7})
             (info "Collection sharded")
             (info (with-out-str
                     (pprint (c/admin-command! conn {:listShards 1}))))))
         (catch com.mongodb.MongoNotPrimaryException e
           ; sigh, why is this a thing
           nil)))

  (invoke! [this test op]
    (let [txn (:value op)]
      (c/with-errors op
        (timeout 5000 (assoc op :type :info, :error :timeout)
          (let [txn' (if (<= (count txn) 1)
                       ; We can run without a transaction
                       [(apply-mop! conn nil (first txn))]

                       ; We need a transaction
                       (with-open [session (c/start-session conn)]
                         (let [opts (txn-options test)
                               body (c/txn
                                      ;(info :txn-begins)
                                      (mapv (partial apply-mop! conn session)
                                            (:value op)))]
                           (.withTransaction session body opts))))]
            (assoc op :type :ok, :value txn'))))))

  (teardown! [this test])

  (close! [this test]
    (.close conn)))

(defn workload
  "A generator, client, and checker for a list-append test."
  [opts]
  (assoc (list-append/test {:key-count          10
                            :max-txn-length     4
                            :max-writes-per-key (:max-writes-per-key opts)
                            :consistency-models [:strong-snapshot-isolation]})
         :client (Client. nil)))
