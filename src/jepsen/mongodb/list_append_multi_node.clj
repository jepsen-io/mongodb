(ns jepsen.mongodb.list-append-multi-node
  "A variant of the list-append workload which performs transactions across
  *different* nodes, using the same session ID and transaction number across
  all."
  (:require [clojure [pprint :refer [pprint]]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [loopr
                                  with-retry]]
            [elle.list-append :as elle.list-append]
            [jepsen [client :as client]
                    [checker :as checker]
                    [util :as util :refer [timeout
                                           map-vals]]]
            [jepsen.mongodb [client :as c]
                            [list-append :as list-append]]
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

(defn txn!
  "Actually execute a transaction across multiple connections."
  [test primary-conn conns txn]
  (with-open [primary-session (c/start-session primary-conn)]
    (let [opts (list-append/txn-options test txn)]
      ; Start txn
      (c/start-txn! primary-session opts)
      ; Apply each micro-op to a different cloned session
      (let [txn' (mapv (fn apply-mop! [mop]
                         (let [conn (rand-nth conns)
                               db   (c/db conn list-append/db-name test)]
                           (with-open [session (c/start-session conn)]
                             (c/with-session-like [session primary-session]
                               (list-append/apply-mop! test db session mop)))))
                       txn)]
        ; And commit
        (with-open [session (c/start-session (rand-nth conns))]
          (c/with-session-like [session primary-session]
            (c/notify-message-sent! session)
            (c/commit-txn! session)))))))

(defrecord Client [conns]
  client/Client
  (open! [this test node]
    (assoc this :conns (->> ;(:nodes test)
                            ; We set up n connections to the *same* node. This
                            ; means we don't get to see what happens when we
                            ; split a transaction across old and new primaries,
                            ; but if we don't do this almost every txn is
                            ; doomed to failure because ONLY a primary can do
                            ; transaction stuff, and the chances that every op
                            ; rolls the dice correctly and hits a primary are
                            ; not good.
                            (repeat 3 node)
                            (mapv (fn [node]
                                    (c/open node test))))))

  (setup! [this test]
    (list-append/create-coll! test (first conns)))

  (invoke! [this test op]
    (let [txn (:value op)]
      (c/with-errors op
        (timeout 5000 (assoc op :type :info, :error :timeout)
          (let [conn (rand-nth conns)
                txn' (if (and (<= (count txn) 1)
                              (not (:singleton-txns test)))
                       ; We can run without a txn
                       (let [db (c/db conn list-append/db-name test)]
                         [(list-append/apply-mop! test db nil (first txn))])

                       (txn! test conn conns txn))]
            (assoc op :type :ok, :value txn'))))))

  (teardown! [this test])

  (close! [this test]
    (mapv c/close! conns)))

(defn workload
  "Same options as for list-append/workload, but uses our custom client."
  [opts]
  (-> (list-append/workload opts)
      (assoc :client (Client. nil))))
