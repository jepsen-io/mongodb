(ns jepsen.mongodb.set
  "A big ol set test: lots of inserts, followed by a final read."
  (:refer-clojure :exclude [test])
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.tools.logging :refer [info debug warn]]
            [jepsen
             [util :as util :refer [meh timeout]]
             [client :as client]
             [checker :as checker]
             [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.mongodb.core :refer :all]
            [jepsen.mongodb.mongo :as m]))

(defrecord Client [db-name coll-name client coll]
  client/Client
  (setup! [this test node]
    (let [client (m/client node)
          coll  (-> client
                    (m/db db-name)
                    (m/collection coll-name)
                    (m/with-read-concern :linearizable)
                    (m/with-write-concern :majority))]
      (assoc this :client client, :coll coll)))

  (invoke! [this test op]
    (with-errors op #{:read}
      (case (:f op)
        :add (let [res (m/insert! coll {:value (:value op)})]
               (assoc op :type :ok))
        :read (assoc op
                     :type :ok
                     :value (->> coll
                                 m/find-all
                                 (map :value)
                                 (into (sorted-set)))))))

  (teardown! [this test]
    (.close ^java.io.Closeable client)))

(defn client
  "A set test client"
  [opts]
  (Client. "jepsen" "set" nil nil))

(defn test
  "A set test, which inserts a sequence of integers into a collection, and
  performs a final read back."
  [opts]
  (test-
    "set"
    (merge
      {:client (client opts)
       :concurrency (count (:nodes opts))
       :generator (->> (range)
                       (map (fn [x] {:type :invoke, :f :add, :value x}))
                       gen/seq
                       (gen/stagger 1/2))
       :final-generator (gen/each
                          (gen/once {:type :invoke, :f :read, :value nil}))
       :checker (checker/compose
                  {:set      checker/set
                   :timeline (timeline/html)
                   :perf     (checker/perf)})}
      opts)))
