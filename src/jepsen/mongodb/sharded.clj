(ns jepsen.mongodb.sharded
  "TODO Namespace documentation for this"
  (:refer-clojure :exclude [test])
  (:require [jepsen.mongodb
             [core :as core]
             [dbutil :as mdbutil]
             [control :as mcontrol]
             [cluster :as mc]
             [util :as mu]
             [time :as mt]
             [mongo :as m]]
            [jepsen
             [control :as c]
             [client  :as client]
             [checker :as checker]
             [db      :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [util    :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [clj-time.core :as time]
            [clj-time.local :as time.local]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [knossos.model :as model])
  (:import [java.util.concurrent Semaphore
            TimeUnit]))

(defn init-configsvr! [test node]
  (c/sudo (:username test)
          (mcontrol/exec test
                         :echo (-> "mongod-configsvr.conf" io/resource slurp
                                   (str/replace #"%PATH_PREFIX%" (mu/path-prefix test node)))
                         :> (mu/path-prefix test node "/mongod-configsvr.conf"))))

(defn init-shardsvr! [test node idx repl-set]
  (c/sudo (:username test)
          (mcontrol/exec test
                         :echo (-> (str "mongod-shardsvr.conf") io/resource slurp
                                   (str/replace #"%ENABLE_MAJORITY_READ_CONCERN%"
                                                (str (= (:read-concern test) :majority)))
                                   (str/replace #"%PATH_PREFIX%" (mu/path-prefix test node))
                                   (str/replace #"%REPL_SET%" repl-set)
                                   (str/replace #"%DB_PATH%" (str "data" idx))
                                   (str/replace #"%STORAGE_ENGINE%" (:storage-engine test)))
                         :> (mu/path-prefix test node (str "/mongod-shardsvr" idx ".conf")))))

(defn format-configsvrs [nodes]
  (str/join "," (map #(str (name %) ":27019") nodes)))

(defn init-mongos! [test node]
  (c/sudo (:username test)
          (mcontrol/exec test
                         :echo (-> "mongos.conf" io/resource slurp
                                   (str/replace #"%PATH_PREFIX%" (mu/path-prefix test node))
                                   (str/replace #"%CONFIGSVRS%" (format-configsvrs (:nodes test))))
                         :> (mu/path-prefix test node "/mongos.conf"))))

(defn start-daemon!
  [clock test node {:keys [pidfile process-name port configfile]}]
  (apply mc/start-daemon! test
         {:chdir (mu/path-prefix test node)
          :background? false
          :logfile (mu/path-prefix test node "/stdout.log")
          :make-pidfile? false
          :match-executable? false
          :match-process-name? true
          :pidfile (mu/path-prefix test node pidfile)
          :process-name process-name}
         (conj (mt/wrap! clock test (mu/path-prefix test node (str "/bin/" process-name)))
               :--fork
               :--pidfilepath (mu/path-prefix test node pidfile)
               :--port port
               :--config (mu/path-prefix test node configfile))))

;; TODO Not compatible outside :vm. The mongo.util stop/kill-daemon is not working in this context
;       for reasons I haven't been able to figure out yet.
(defn kill-all! [test node]
  (let [shard-count (:shard-count test)]
    (cu/stop-daemon! (mu/path-prefix test node "/mongos.pid"))
    (cu/stop-daemon! (mu/path-prefix test node "/mongod-configsvr.pid"))
    (doseq [idx (range 0 shard-count)]
      (cu/stop-daemon! (mu/path-prefix test node (str "/mongod-shardsvr" idx ".pid"))))))

(defn db
  [clock url {:keys [mongos-sem chunk-size shard-count]}]
  (let [state (atom {})]
    (reify db/DB
      (setup! [_ test node]
        (swap! state assoc node {:setup-called true})
        (util/timeout 300000
                      (throw (RuntimeException.
                              (str "Mongo setup on " node " timed out!")))

                      (when (= :vm (:virt test))
                        (debian/install [:libc++1 :libsnmp30]))

                      ;; Install MongoDB Package
                      (->> (or (some->> (:mongodb-dir test)
                                        io/file
                                        .getCanonicalPath
                                        (str "file://"))
                               url)
                           (mdbutil/install! test node))

                      ;; Setup configsvr replset
                      (init-configsvr! test node)
                      (start-daemon! clock test node {:pidfile "/mongod-configsvr.pid"
                                                      :process-name "mongod"
                                                      :port 27019
                                                      :configfile "/mongod-configsvr.conf"})
                      (core/join! test node {:port 27019 :repl-set-name "configsvr"})

                      ;; Nodes spawn a router for the setup phase.
                      (init-mongos! test node)
                      (start-daemon! clock test node {:pidfile "/mongos.pid"
                                                      :process-name "mongos"
                                                      :port 27017
                                                      :configfile "/mongos.conf"})

                      ;; Setup `shard-count` shardsvr replsets
                      (doseq [idx (range 0 shard-count)]
                        ;; Port starts at 27020
                        (let [port (+ 27020 idx)
                              ;; Each replset gets its own name by its index
                              repl-set (str "jepsen" idx)
                              filename (str "/mongod-shardsvr" idx)]
                          (mt/init! clock test)
                          (init-shardsvr! test node idx repl-set)
                          (start-daemon! clock test node {:pidfile (str filename ".pid")
                                                          :process-name "mongod"
                                                          :port port
                                                          :configfile (str filename ".conf")})
                          (core/join! test node {:port port :repl-set-name repl-set})

                          ;; Add shard to cluster with the most recently
                          (m/admin-command! (m/client node 27017)
                                            :addShard (str "jepsen" idx "/" node ":" port))))

                      ;; Configure the cluster
                      (let [conn (m/client node 27017)
                            coll (m/collection (m/db conn "config") "settings")]
                        ;; Wrapped with meh to swallow errors from repeated calls. It's not
                        ;; really what we want but ok.
                        (util/meh (m/admin-command! conn :enableSharding "jepsen"))
                        (util/meh (m/admin-command! conn
                                                    :shardCollection "jepsen.sharded"
                                                    :key {:_id 1}))

                        ;; Set chunk size, defaults to 64MB
                        (m/upsert! coll {:_id "chunksize" :value chunk-size})

                        ;; Nodes race to acquire `--mongos-count` locks. If they do not acquire a
                        ;; lock, they tear down the router they used for setup, just like a
                        ;; game of musical chairs.
                        (when-not (.tryAcquire mongos-sem)
                          (cu/stop-daemon! (mu/path-prefix test node "/mongos.pid"))))))

      (teardown! [_ test node]
        (if-not (:setup-called (get @state node))
          (kill-all! test node)
          (do (mdbutil/snarf-logs! test node)
              (try
                (kill-all! test node)
                (finally (mdbutil/snarf-logs! test node)))))))))

(defrecord Client [db-name coll-name read-concern write-concern client coll]
  client/Client
  (open! [this test node]
    (let [client (m/client node)
          coll   (-> client
                     (m/db db-name)
                     (m/collection coll-name)
                     (m/with-read-concern read-concern)
                     (m/with-write-concern write-concern))]
      (assoc this :client client :coll coll)))

  (invoke! [this test op]
    (core/with-errors op #{:read}
      (case (:f op)
        :add (let [res (m/insert! coll {:value (:value op)})]
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

(defn set-client
  [opts]
  (Client. "jepsen" "sharded"
           (:read-concern opts)
           (:write-concern opts)
           nil nil))

(defrecord RegisterClient [db-name
                           coll-name
                           read-concern
                           write-concern
                           read-with-find-and-modify
                           client
                           coll]
  client/Client
  (open! [this test node]
    (let [client (m/client node)
          coll   (-> client
                     (m/db db-name)
                     (m/collection coll-name)
                     (m/with-read-concern read-concern)
                     (m/with-write-concern write-concern))]
      (assoc this :client client :coll coll)))

  (invoke! [this test op]
    (core/with-errors op #{:read}
      (let [id    (key (:value op))
            value (val (:value op))]
        (case (:f op)
          :read (let [doc (if read-with-find-and-modify
                            ;; CAS read
                            (m/read-with-find-and-modify coll id)
                            ;; Normal read
                            (m/find-one coll id))]
                  (assoc op
                         :type  :ok
                         :value (independent/tuple id (:value doc))))

          :write (let [res (m/upsert! coll {:_id id, :value value})]
                   ;; Note that modified-count could be zero, depending on the
                   ;; storage engine, if you perform a write the same as the
                   ;; current value.
                   (assert (< (:matched-count res) 2))
                   (assoc op :type :ok))

          :cas   (let [[value value'] value
                       res (m/cas! coll
                                   {:_id id, :value value}
                                   {:_id id, :value value'})]
                   ;; Check how many documents we actually modified.
                   (condp = (:matched-count res)
                     0 (assoc op :type :fail)
                     1 (assoc op :type :ok)
                     true (assoc op :type :info
                                 :error (str "CAS: matched too many docs! "
                                             res))))))))

  (close! [this test]
    (.close ^java.io.Closeable client))

  (setup! [_ _])
  (teardown! [_ _]))

(defn register-client
  [opts]
  (RegisterClient. "jepsen" "sharded"
           (:read-concern opts)
           (:write-concern opts)
           (:read-with-find-and-modify opts)
           nil nil))

(defn nemesis-id
  "Takes a test map and returns a document ID to move.
  Nemeses don't have a view of the current op ID so we look at how much time
  has elapsed since the test started and do some guesswork based on expected
  throughput to get a document ID for chunk moves."
  [{:keys [start-time]} model]
  (case model
    :set (let [elapsed (time/interval start-time (time.local/local-now))]
           (* 2 (time/in-seconds elapsed)))
    :register (rand-int 8)))

(defn balancer-nemesis [conns model]
  (reify nemesis/Nemesis
    (setup! [this test]
      (balancer-nemesis
       ;; TODO you just need 1 valid router here, not all of them
       ;;      make sure you always get a valid router when (< mongos-count nodes)
       ;;      try nodes in sequence until you get a good connection
       (into {} (util/real-pmap (juxt identity core/await-conn) (:nodes test)))))

    (invoke! [this test op]
        (case (:f op)
          :move (let [[node conn] (nth (vec conns) (-> test :nodes count rand-int))
                      dest-replset (str "jepsen" (rand-int (:shard-count test)))
                      id (nemesis-id test model)]
                  (m/admin-command! conn
                                    :moveChunk "jepsen.sharded"
                                    :find {:_id id}
                                    :to dest-replset)
                  (assoc op :value [:moving-chunk-with id :to dest-replset]))))))

(defn sharded-nemesis [model]
  (nemesis/compose
   {#{:start :stop} (nemesis/partition-random-halves)
    #{:move} (balancer-nemesis nil model)}))

(defn shard-migration-gen []
  (gen/seq (cycle [(gen/sleep 10)
                   {:type :info, :f :move}
                   {:type :info, :f :start}
                   (gen/sleep 20)
                   {:type :info, :f :stop}])))

(defn ensure-shard-count [opts]
  (when (< (:shard-count opts) 1)
    (throw (IllegalArgumentException. "Sharded tests must be run with a --shard-count of 1 or higher"))))

(defn set-test
  "Tests against a sharded mongodb cluster. We insert documents against
  the mongos router while inducing shard migrations and partitioning the
  network."
  [opts]
  (ensure-shard-count opts)
  (let [mongos-sem (Semaphore. (or (:mongos-count opts) (count (:nodes opts))))]
    (core/mongodb-test
     "sharded-set"
     (merge
      opts
      {:client (set-client opts)
       :nemesis (sharded-nemesis 2)
       :generator (gen/phases
                   (->> (range)
                        (map (fn [x] {:type :invoke, :f :add, :value x}))
                        gen/seq
                        (gen/stagger 1/2)
                        (gen/nemesis
                         (shard-migration-gen))
                        (gen/time-limit (:time-limit opts)))
                   (gen/nemesis
                    (gen/once {:type :info, :f :stop, :value nil}))
                   (gen/sleep 40)
                   (gen/clients (gen/each
                                 (gen/limit 2 {:type :invoke, :f :read, :value nil}))))
       :db (db (:clock opts)
               (:tarball opts)
               {:mongos-sem  mongos-sem
                :chunk-size  (:chunk-size opts)
                :shard-count (:shard-count opts)})
       :checker (checker/compose
                 {:set (checker/set)
                  :timeline (timeline/html)
                  :perf (checker/perf)})}))))

;; Generators
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 5)})
(defn r   [_ _] {:type :invoke, :f :read})
(defn cas [_ _] {:type :invoke, :f :cas, :value [(rand-int 5) (rand-int 5)]})

(defn register-test
  "Tests against a sharded mongodb cluster. We insert documents against
  the mongos router while inducing shard migrations and partitioning the
  network."
  [opts]
  (ensure-shard-count opts)
  (let [mongos-sem (Semaphore. (or (:mongos-count opts) (count (:nodes opts))))]
    (core/mongodb-test
     "sharded-register"
     (merge
      opts
      {:client (register-client opts)
       :nemesis (sharded-nemesis :register)
       :generator (->> (independent/concurrent-generator
                        10
                        (range)
                        (fn [k]
                          (->> (gen/mix [w cas cas])
                               (gen/reserve 5 (if (:no-reads opts)
                                                (gen/mix [w cas cas])
                                                r))
                               (gen/time-limit (:key-time-limit opts)))))
                       (gen/stagger 1)
                       (gen/nemesis (shard-migration-gen))
                       (gen/time-limit (:time-limit opts)))
       :db (db (:clock opts)
               (:tarball opts)
               {:mongos-sem  mongos-sem
                :chunk-size  (:chunk-size opts)
                :shard-count (:shard-count opts)})
       :model (model/cas-register)
       :checker (checker/compose
                 {:linear  (independent/checker (checker/linearizable))
                  :timeline (independent/checker (timeline/html))
                  :perf     (checker/perf)})}))))
