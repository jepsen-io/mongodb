(ns jepsen.mongodb.dbhash
  "Client, generator, and checker for verifying data consistency across all
  members of a replica set by using the 'dbHash' command."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [error]]
            [jepsen.checker :as checker]
            [jepsen.client :as client]
            [jepsen.control :as c]
            [jepsen.generator :as gen]
            [jepsen.nemesis :as nemesis]
            [jepsen.mongodb.control :as mcontrol]
            [jepsen.mongodb.mongo :as m]
            [jepsen.mongodb.util :as mu]))

(defn- mongo-shell-command [node]
  "Returns a series of JavaScript statements in their stringified form for
  performing a dbhash check against a replica set. We make sure the replica set
  has stabilized with node #0 as the primary because our Jepsen tests give node
  #0 the highest priority."
  (->> [(format "var rst = new ReplSetTest('%s')" (str (m/server-address node)))
        "rst.waitForState(rst.nodes[0], ReplSetTest.State.PRIMARY)"
        "rst.checkReplicatedDataHashes()"]
       (str/join "; ")))

(defn- check-replica-set!
  "Performs a dbhash check against the test's replica set. This function returns
  an updated op indicating whether the dbhash check succeeded or failed."
  [test op]
  ; We use the first node as the seed node to ReplSetTest.
  (let [node (first (:nodes test))]
    (try
      ; If the dbhash check fails, then a JavaScript exception will be thrown,
      ; which in turn will cause the mongo shell to exist with a non-zero return
      ; code and `jepsen.mongodb.control/exec` to throw a RuntimeException.
      (c/with-session node (get (:sessions test) node)
        (->> node
             mongo-shell-command
             (mcontrol/exec test
               (mu/path-prefix test node "/bin/mongo")
               :--nodb :--quiet :--eval)))

      (assoc op :result :valid)
      (catch RuntimeException e
        ; Capture the mongo shell's output on failure.
        (assoc op
               :result :invalid
               :fail-msg (.getMessage e))))))

(def gen
  "Generator for the :compare-dbhashes operation."
  (gen/once {:type :info, :f :compare-dbhashes, :value nil}))

(def client
  "Client that implements the jepsen.client/Client protocol and runs the dbhash
  check against a replica set."
  (reify client/Client
    (open! [client test node] client)
    (close! [client test])
    (setup! [client test])
    (invoke! [client test op]
      (when (= :compare-dbhashes (:f op))
        (check-replica-set! test op)))
    (teardown! [client test])))

(def nemesis
  "Client that implements the jepsen.nemesis/Nemesis protocol and runs the
  dbhash check against a replica set."
  (reify nemesis/Nemesis
    (setup! [nemesis test] nemesis)
    (invoke! [nemesis test op]
      (when (= :compare-dbhashes (:f op))
        (check-replica-set! test op)))
    (teardown! [nemesis test])))

(def checker
  "Checker for reporting the results of the dbhash check."
  (reify checker/Checker
    (check [this test model history opts]
      (if-let [dbhash-result
               (->> history
                    (filter (fn [op] (= :compare-dbhashes (:f op))))
                    ; We only want the result.
                    (filter (fn [op] (contains? op :result)))
                    ; Will be nil if the op doesn't exist.
                    first)]
        (case (:result dbhash-result)
          :valid    {:valid? true}
          :invalid  (do (error "dbhash check failed" (:fail-msg dbhash-result))
                        {:valid? false
                         :error  (str "see the earlier 'dbhash check failed'"
                                      " message in the logs along with the"
                                      " mongo shell's output")}))))))
