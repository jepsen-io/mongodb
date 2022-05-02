(ns jepsen.mongodb.list-append-test
  (:require [clojure [pprint :refer [pprint]]
                     [test :refer :all]]
            [jepsen [checker :as checker]
                    [util :as util]]
            [jepsen.mongodb.list-append :as la]))

(deftest stale-read-test
  ; Making sure that we can catch stale reads with our checker
  (let [ax1  {:index 0, :time 0, :process 0, :type :invoke, :f :txn, :value [[:append :x 1]]}
        ax1' {:index 1, :time 1, :process 0, :type :ok,     :f :txn, :value [[:append :x 1]]}
        rx   {:index 2, :time 2, :process 1, :type :invoke, :f :txn, :value [[:r :x nil]]}
        rx'  {:index 3, :time 3, :process 1, :type :ok,     :f :txn, :value [[:r :x nil]]}
        history [ax1 ax1' rx rx']
        test    (-> {:name "stale read test"
                     :start-time (util/local-time)
                     :history history}
                    (merge (la/workload {})))
        results (checker/check (:checker test) test history {})]
    ;(pprint results)
    (is (= #{:strong-snapshot-isolation} (->> results :elle :not)))
    (is (= #{:G-single-realtime} (->> results :elle :anomalies keys set)))))
