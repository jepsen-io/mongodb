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
             [nemesis :as nemesis]
             [util    :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.util.concurrent Semaphore]))

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

(defn db [clock url mongos-sem chunk-size shard-count]
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
                           #(mdbutil/install! test node % shard-count))

                      ;; configsvr
                      (init-configsvr! test node)
                      (start-daemon! clock test node {:pidfile "/mongod-configsvr.pid"
                                                      :process-name "mongod"
                                                      :port 27019
                                                      :configfile "/mongod-configsvr.conf"})
                      (core/join! test node {:port 27019 :repl-set-name "configsvr"})


                      ;; Nodes race to acquire `mongos-count` locks and spawn a router if acquired
                      (when (.tryAcquire mongos-sem)
                        (init-mongos! test node)
                        (start-daemon! clock test node {:pidfile "/mongos.pid"
                                                        :process-name "mongos"
                                                        :port 27017
                                                        :configfile "/mongos.conf"}))

                      ;; shardsvr
                      (doseq [idx (range 0 shard-count)]
                        ;; Port starts at 27020
                        (let [port (+ 27020 idx)
                              ;; Each replset gets its own name by its index
                              repl-set (str "jepsen" idx)]
                          (mt/init! clock test)
                          (init-shardsvr! test node idx repl-set)
                          (start-daemon! clock test node {:pidfile (str "/mongod-shardsvr" idx ".pid")
                                                          :process-name "mongod"
                                                          :port port
                                                          :configfile "/mongod-shardsvr.conf"})
                          (core/join! test node {:port port :repl-set-name repl-set})
                          (m/admin-command! (m/client node 27017)
                                            :addShard (str "jepsen" idx "/" node ":" port))))

                      (let [conn (m/client node 27017)
                            coll (m/collection (m/db conn "config") "settings")]
                        ;; Wrapped with meh to swallow errors from repeated calls. It's not
                        ;; really what we want but ok.
                        (util/meh (m/admin-command! conn :enableSharding "jepsen"))
                        (util/meh (m/admin-command! conn
                                                    :shardCollection "jepsen.sharded-set"
                                                    :key {:_id 1}))

                        ;; Set chunk size, defaults to 64MB
                        (m/upsert! coll {:_id "chunksize" :value chunk-size}))))

      (teardown! [_ test node]
        (if-not (:setup-called (get @state node))
          (kill-all! test node)
          (do (mdbutil/snarf-logs! test node)
              (try
                (kill-all! test node)
                (finally (mdbutil/snarf-logs! test node)))))))))

;; TODO Fix client interaction when (< mongos (count nodes))
(defrecord Client [db-name coll-name read-concern write-concern client coll]
  client/Client
  (open! [this test node]
    (let [client (m/client node)
          coll (-> client
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

(defn client
  [opts]
  (Client. "jepsen" "sharded-set"
           (:read-concern opts)
           (:write-concern opts)
           nil nil))

(defn test
  "Tests against a sharded mongodb cluster. We insert documents against
  the mongos router while inducing shard migrations and partitioning the
  network."
  [opts]
  (let [nodes (count (:nodes opts))
        mongos-sem (Semaphore. (or (:mongos-count opts) nodes))]
    (core/mongodb-test
     "sharded-set"
     (merge
      opts
      {:client (client opts)
       :concurrency nodes
       :generator (->> (range)
                       (map (fn [x] {:type :invoke, :f :add, :value x}))
                       gen/seq
                       (gen/stagger 1/2))
       :final-generator (gen/each
                         (gen/limit 2 {:type :invoke, :f :read, :value nil}))
       :db (db (:clock opts)
               (:tarball opts)
               mongos-sem
               (:chunk-size opts)
               (:shard-count opts))
       :nemesis nemesis/noop
       :checker (checker/compose
                 {:set (checker/set)
                  :timeline (timeline/html)
                  :perf (checker/perf)})}))))
