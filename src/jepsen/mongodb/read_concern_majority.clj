(ns jepsen.mongodb.read-concern-majority
  "Read Concern Majority Test.

  OVERVIEW:

  readConcern majority should provide the guarantee that any data returned is
  the result of committed operations, i.e. operations that are present on a
  majority of the nodes in the replica set, and therefore should never be rolled
  back. To check this, we verify the following property, which should hold given
  the restriction that we consider only document insert operations; no updates
  or deletes.

  If R is a readConcern majority read that occurs at time T (from the
  perspective of a fixed client) and reads the set of documents D from a
  collection C, then any read R' that occurs at time T’ >= T, should read a set
  of  documents D' such that D is a subset of D', i.e. every document that
  exists in D should exist in D'.


  TEST SETUP:

  In order to check the above property, this test continually inserts unique
  documents, on potentially many writer threads, while a single thread
  periodically reads the full state of the collection. When the test completes,
  we verify that the set of documents returned by every read R is a subset of
  the documents returned by the subsequent read, R'. Note that 'subsequent' in
  this context has a well defined meaning since reads occur within a single
  thread of execution.

  e.g.

    Reader/Writer Threads:

    * = insert operation
    R = read operation

    Writer-1 ––*––––––––––*––––––––––*–––––*––––––––––>
    Writer-2 –––––––*––––––––––––*–––––––––––––*––––––>
    Writer-3 ––*–––––––––*––––––––––––––*––––––––*––––>
      .
      .
      .
    Writer-N ––––––––––*––––––––––*––––––––––––*––––––>
    Reader   –––––––R–––––––––R–––––––––R–––––––––R–––>
  "
  (:refer-clojure :exclude [test])
  (:require [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]]
            [jepsen.checker.timeline :as timeline]
            [knossos [op :as op]]
            [jepsen.mongodb.core :refer :all]
            [jepsen.mongodb.mongo :as m]))

(defrecord Client [db-name coll-name read-concern write-concern client coll]
  client/Client
  (open! [this test node]
    ; Initiate a single client connection to a MongoDB database collection so we
    ; can use it for invoking test operations.
    (let [client (m/client node)
          ; "->" is like a pipe operator, allowing you to thread a value through
          ; multiple function calls linearly.
          coll (-> client
                   (m/db db-name)
                   (m/collection coll-name)
                   (m/with-read-concern read-concern)
                   (m/with-write-concern write-concern))]
      (assoc this :client client :coll coll)))

  (invoke! [this test op]
    ; Given an operation 'op', execute either a document insert (:add), or a
    ; findAll() on the test collection (:read). A successful :read updates
    ; (:value op) with the set of all document values returned. If a database
    ; operation fails, it will handle the error and return an op with :type
    ; :fail, populated with the proper error code.
    (with-errors op #{:read}
      (case (:f op)
        :add (do (m/insert! coll (select-keys op [:value]))
                 (assoc op :type :ok))

        :read (assoc op
                     :type :ok
                     :value (->> coll
                                 m/find-all
                                 (map :value)
                                 (into (sorted-set)))))))

  (close! [this test]
    (.close ^java.io.Closeable client))

  (setup! [_ _])
  (teardown! [_ _]))

(defn subset-chain?
  "Determine if a sequence of sets form an ordered chain of subsets.

      (subset-chain? [#{1 2 3} #{1 2 3 4} #{1 2 3 4 5 6}]) == true
      (subset-chain? [#{4 5} #{4 5 6} #{4 5 8}]) == false
      (subset-chain? []) == true
      (subset-chain? [#{1 2}]) == true

  An empty sequence or a sequence with a single set are both considered
  trivially valid subset chains."
  [sets]
  (let [overlapping-pairs (partition 2 1 sets)
        subset?-pairs (map (partial apply clojure.set/subset?)
                           overlapping-pairs)]
       (every? true? subset?-pairs)))

(defn rcmajority-checker
  "Given a set of :add operations interspersed with periodic :read ops, verify
  that for every read R that reads a set of elements D, any later read R' reads
  elements D', where D is a subset of D'."
  []
  (reify checker/Checker
    (check [this test model history opts]
      (let [reads (->> history
                       (filter op/ok?)
                       (filter #(= :read (:f %)))
                       (sort-by :time)
                       (map :value))]
          {:valid? (subset-chain? reads)}))))

(defn client
  "A read concern majority test client."
  [opts]
  (Client. "jepsen"                 ; database name
           "read-concern-majority"  ; collection name
           :majority                ; readConcern
           (:write-concern opts)
           nil nil))

(defn write-gen
  "Write workload generator."
  [mean-delay] (->> (range)
                    (map (fn [x] {:type :invoke, :f :add, :value x}))
                    gen/seq
                    (gen/stagger mean-delay)))

(defn read-gen
  "Read workload generator."
  [mean-delay] (->> (range)
                    (map (fn [x] {:type :invoke, :f :read, :value nil}))
                    gen/seq
                    (gen/stagger mean-delay)))

;; Average delays between consecutive ops.
(def read-interval-secs 8)
(def write-interval-secs 1/2)

(defn test
  "A read concern majority test. We continuously insert documents on many
  threads while periodically reading the full state of the collection with a
  single, dedicated reader thread, in order to ensure that our reads are
  causally ordered."
  [opts]
  (mongodb-test
    "read-concern-majority"
    (merge
      {:client (client opts)
       :read-concern :majority
       :concurrency  (count (:nodes opts))
       ; Reserve a single thread for reads and let all other threads do writes.
       :generator (gen/reserve 1 (read-gen read-interval-secs)
                                 (write-gen write-interval-secs))
       :final-generator nil
       :checker (checker/compose
                  {:read-concern-majority (rcmajority-checker)
                   :timeline              (timeline/html)
                   :perf                  (checker/perf)})}
      ;; The clients for this test always uses a :majority read concern, so we
      ;; make sure that the options for the MongoDB test are also set to
      ;; :majority read concern, so that the mongod processes are started with
      ;; --enableMajorityReadConcern.
      (dissoc opts :read-concern))))
