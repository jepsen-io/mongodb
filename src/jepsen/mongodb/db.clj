(ns jepsen.mongodb.db
  "Database setup and automation."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh random-nonempty-subset]]]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb [client :as client]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def log-file "/var/log/mongodb/mongod.log")
(def data-dir "/var/lib/mongodb")

(def mongos-dir "/tmp/mongos")
(def mongos-log-file "/var/log/mongodb/mongos.stdout")
(def mongos-pid-file (str mongos-dir "/mongos.pid"))
(def mongos-bin "mongos")

(def subpackages
  "MongoDB has like five different packages to install; these are the ones we
  want."
  ["mongos"
   "server"])

(defn deb-url
  "What's the URL of the Debian package we install?"
  [test subpackage]
  (let [version (:version test)]
    ; TODO: sort out the 4.2 in the URL here
    (str "https://repo.mongodb.org/apt/debian/dists/stretch/mongodb-org/4.2/main/binary-amd64/mongodb-org-" subpackage "_" version "_amd64.deb")))

(defn install!
  [test]
  "Installs MongoDB on the current node."
  (c/su
    (c/exec :mkdir :-p "/tmp/jepsen")
    (c/cd "/tmp/jepsen"
          (doseq [subpackage subpackages]
            (when-not (= (:version test)
                         (debian/installed-version (str "mongodb-org-"
                                                        subpackage)))
              (let [file (cu/wget! (deb-url test subpackage))]
                (info "Installing" subpackage (:version test))
                (c/exec :dpkg :-i :--force-confnew file))
              (c/exec :systemctl :daemon-reload))))))

(defn config-server?
  "Takes a test map, and returns true iff this set of nodes is intended to be a
  configsvr."
  [test]
  (= (:replica-set-name test) "rs_config"))

(defn configure!
  "Sets up configuration files"
  [test node]
  (c/su
    (c/exec :echo :> "/etc/mongod.conf"
            (-> (slurp (io/resource "mongod.conf"))
                (str/replace "%REPL_SET_NAME%"
                             (:replica-set-name test "rs_jepsen"))
                (str/replace "%CLUSTER_ROLE%"
                             (if (config-server? test)
                               "configsvr"
                               "shardsvr"))))))

(defn start!
  "Starts mongod"
  [test node]
  (c/su (c/exec :systemctl :start :mongod)))

(defn stop!
  "Stops the mongodb service"
  [test node]
  (try+
    (c/su (c/exec :systemctl :stop :mongod))
    (catch [:exit 5] e
      ; Not loaded; we probably haven't installed
      )))

(defn wipe!
  "Removes logs and data files"
  [test node]
  (c/su (c/exec :rm :-rf log-file (c/lit (str data-dir "/*")))))

;; Replica sets

(defn target-replica-set-config
  "Generates the config for a replset in a given test."
  [test]
  {:_id (:replica-set-name test "rs_jepsen")
   :configsvr (config-server? test)
   ; See https://docs.mongodb.com/manual/reference/replica-configuration/#rsconf.settings.catchUpTimeoutMillis
   :settings {:heartbeatTimeoutSecs       1
              :electionTimeoutMillis      1000
              :catchUpTimeoutMillis       1000
              :catchUpTakeoverDelayMillis 3000}
   :members (->> test
                 :nodes
                 (map-indexed (fn [i node]
                                {:_id  i
                                 :priority (- (count (:nodes test)) i)
                                 :host     (str node ":"
                                                (if (config-server? test)
                                                  client/config-port
                                                  client/shard-port))})))})

(defn replica-set-initiate!
  "Initialize a replica set on a node."
  [conn config]
  (client/admin-command! conn {:replSetInitiate config}))

(defn replica-set-config
  "Returns the current repl set config"
  [conn]
  (client/admin-command! conn {:replSetConfig 1}))

(defn replica-set-status
  "Returns the current replica set status."
  [conn]
  (client/admin-command! conn {:replSetGetStatus 1}))

(defn primaries
  "What nodes does this conn think are primaries?"
  [conn]
  (->> (replica-set-status conn)
       :members
       (filter #(= "PRIMARY" (:stateStr %)))
       (map :name)
       (map client/addr->node)))

(defn primary
  "Which single node does this conn think the primary is? Throws for multiple
  primaries, cuz that sounds like a fun and interesting bug, haha."
  [conn]
  (let [ps (primaries conn)]
    (when (< 1 (count ps))
      (throw (IllegalStateException.
               (str "Multiple primaries known to "
                    conn
                    ": "
                    ps))))

    (first ps)))

(defn await-join
  "Block until all nodes in the test are known to this connection's replset
  status"
  [test conn]
  (while (not= (set (:nodes test))
               (->> (replica-set-status conn)
                    :members
                    (map :name)
                    (map client/addr->node)
                    set))
    (info :replica-set-status
          (with-out-str (->> (replica-set-status conn)
                                                 :members
                                                 (map :name)
                                                 (map client/addr->node)
                                                 sort
                                                 pprint)
                        (prn :test (sort (:nodes test)))))
    (Thread/sleep 1000)))

(defn await-primary
  "Block until a primary is known to the current node."
  [conn]
  (while (not (primary conn))
    (Thread/sleep 1000)))

(defn join!
  "Joins nodes into a replica set. Intended for use during setup."
  [test node]
  (let [port (if (config-server? test)
               client/config-port
               client/shard-port)]
    ; Wait for all nodes to be reachable
    (.close (client/await-open node port))
    (jepsen/synchronize test)

    ; Start RS
    (when (= node (jepsen/primary test))
      (with-open [conn (client/open node port)]
        (info "Initiating replica set on" node "\n"
              (with-out-str (pprint (target-replica-set-config test))))
        (replica-set-initiate! conn
                               (target-replica-set-config test))

        (info "Waiting for cluster join")
        (await-join test conn)

        (info "Waiting for primary election")
        (await-primary conn)
        (info "Primary ready")))

    ; For reasons I really don't understand, you have to prevent other nodes
    ; from checking the replset status until *after* we initiate the replset on
    ; the primary--so we insert a barrier here to make sure other nodes don't
    ; wait until primary initiation is complete.
    (jepsen/synchronize test 300)

    ; For other reasons I don't understand, you *have* to open a new set of
    ; connections after replset initation. I have a hunch that this happens
    ; because of a deadlock or something in mongodb itself, but it could also
    ; be a client connection-closing-detection bug.

    ; Amusingly, we can't just time out these operations; the client appears to
    ; swallow thread interrupts and keep on doing, well, something. FML.
    (with-open [conn (client/open node port)]
      (info "Waiting for cluster join")
      (await-join test conn)

      (info "Waiting for primary")
      (await-primary conn)

      (info "Primary is" (primary conn))
      (jepsen/synchronize test 300))))

(defn replica-set-db
  "This database runs a single replica set."
  []
  (reify
    db/DB
    (setup! [db test node]
      (install! test)
      (configure! test node)
      (start! test node)
      (join! test node))

    (teardown! [db test node]
      (stop! test node)
      (wipe! test node))

    db/LogFiles
    (log-files [db test node]
      [log-file])

    db/Process
    (start! [_ test node]
      (start! test node))

    (kill! [_ test node]
      (c/su (cu/grepkill! :mongod))
      (stop! test node))

    db/Pause
    (pause! [_ test node]
      (c/su (cu/grepkill! :stop :mongod)))

    (resume! [_ test node]
      (c/su (cu/grepkill! :cont :mongod)))

    db/Primary
    (setup-primary! [_ test node])

    (primaries [_ test]
      (try (->> (:nodes test)
                (real-pmap (fn [node]
                             (with-open [conn (client/open
                                                node
                                                (if (config-server? test)
                                                  client/config-port
                                                  client/shard-port))]
                               ; Huh, sometimes Mongodb DOES return multiple
                               ; primaries from a single request. Weeeeird.
                               (primaries conn))))
                (reduce concat)
                distinct)
           (catch Exception e
             (info e "Can't determine current primaries")
             nil)))))

;; Sharding

(defn shard-node-plan
  "Takes a test, and produces a map of shard names to lists of nodes
  which form the replica set for that set. We always generate a config replica
  set, and fill remaining nodes with shards.

    {\"config\" [\"n1\" \"n2\" ...]
     \"shard1\" [\"n4\" ...]
     \"shard2\" [\"n7\" ...]}"
  [test]
  (let [n           (:nodes test)
        shard-size  3]
    (assert (< (* 2 shard-size) (count n))
            (str "Need at least " (* 2 shard-size) " nodes for 1 shard"))
    (zipmap (->> (range) (map inc) (map (partial str "shard")) (cons "config"))
            (partition-all shard-size n))))

(defn test-for-shard
  "Takes a test map and a shard map, and creates a version of the test map with
  the replica set name and nodes based on the given shard.

  (test-for-shard test {:nodes [...})"
  [test shard]
  (assoc test
         :nodes             (:nodes shard)
         :replica-set-name  (str "rs_" (:name shard))))

(defn shard-for-node
  "Takes a sharded DB and a node; returns the shard this node belongs to."
  [sharded-db node]
  (first (filter (fn [shard] (some #{node} (:nodes shard)))
                 (:shards sharded-db))))

(defn on-shards
  "Takes a sharded DB. Calls (f shard) in parallel on each
  shard. Returns a map of shard names to the results of f on that shard."
  [sharded-db f]
  (zipmap (map :name (:shards sharded-db))
          (real-pmap f (:shards sharded-db))))

(defn on-shards-nodes
  "Takes a sharded DB. Calls (f shard node) in parallel on each shard and node.
  Returns a map of shards to nodes to the results of f on that shard and node."
  [sharded-db f]
  (on-shards (fn [shard]
               (zipmap (:nodes shard)
                       (real-pmap (partial f shard) (:nodes shard))))))

(defn configure-mongos!
  "Sets up mongos configuration file."
  [test node config-db]
  (c/su
    (c/exec :echo :> "/etc/mongos.conf"
            (-> (slurp (io/resource "mongos.conf"))
                (str/replace "%CONFIG_DB%" config-db)))))

(defn start-mongos!
  "Starts the mongos daemon on the local node."
  [test node]
  (c/su
    (c/exec :mkdir :-p mongos-dir)
    (cu/start-daemon!
      {:logfile mongos-log-file
       :pidfile mongos-pid-file
       :chdir   mongos-dir}
      (str "/usr/bin/" mongos-bin)
      :--config "/etc/mongos.conf")))

(defn stop-mongos!
  "Stops the mongos daemon on the local node."
  [test node]
  (c/su (cu/stop-daemon! mongos-bin mongos-pid-file)))

(defn add-shards!
  "Adds the initial set of shards for the DB setup."
  [node shard-strs]
  (with-open [conn (client/open node client/mongos-port)]
    (doseq [shard shard-strs]
      (info "Adding shard" shard)
      (client/admin-command! conn {:addShard shard}))))

(defrecord Mongos [config-str shard-strs]
  db/DB
  (setup! [this test node]
    (install! test)
    (configure-mongos! test node config-str)
    (start-mongos! test node)
    (info "Waiting for mongos to start")
    (client/await-open node client/mongos-port)
    (jepsen/synchronize test)
    (when (= (jepsen/primary test) node)
      (add-shards! node shard-strs)))

  (teardown! [this test node]
    (stop-mongos! test node)
    (c/su
      (c/exec :rm :-rf mongos-log-file mongos-dir)))

  db/LogFiles
  (log-files [this test node]
    [mongos-log-file]))

(defrecord ShardedDB [mongos shards tcpdump]
  db/DB
  (setup! [this test node]
    (db/setup! tcpdump test node)
    (let [shard (shard-for-node this node)]
      (info "Setting up shard" shard)
      (db/setup! (:db shard) (test-for-shard test shard) node))

    (db/setup! mongos test node))

  (teardown! [this test node]
    (db/teardown! mongos test node)
    (let [shard (shard-for-node this node)]
      (info "Tearing down shard" shard)
      (db/teardown! (:db shard) (test-for-shard test shard) node))
    (db/teardown! tcpdump test node))

  db/LogFiles
  (log-files [this test node]
    (concat (db/log-files tcpdump test node)
            (db/log-files mongos test node)
            (let [shard (shard-for-node this node)]
              (db/log-files (:db shard) (test-for-shard test shard) node))))

  db/Primary
  (setup-primary! [_ test node] nil)
  (primaries [this test]
    (->> (on-shards this
                    (fn [shard]
                      (db/primaries (:db shard)
                                    (test-for-shard test shard))))
         vals
         (reduce concat)
         distinct))

  db/Process
  (start! [this test node]
    (let [shard (shard-for-node this node)]
      (db/start! (:db shard) (test-for-shard test shard) node)))

  (kill! [this test node]
    (let [shard (shard-for-node this node)]
      (db/kill! (:db shard) (test-for-shard test shard) node)))

  db/Pause
  (pause! [this test node]
    (let [shard (shard-for-node this node)]
      (db/pause! (:db shard) (test-for-shard test shard) node)))

  (resume! [this test node]
    (let [shard (shard-for-node this node)]
      (db/resume! (:db shard) (test-for-shard test shard) node))))

(defn sharded-db
  "This database deploys a config server replica set, shard replica sets, and
  mongos sharding servers."
  [opts]
  (let [plan (shard-node-plan opts)]
    (ShardedDB.
      (Mongos.
        ; Config server
        (->> (get plan "config")
             (map #(str % ":" client/config-port))
             (str/join ",")
             (str "rs_config/"))
        ; Shards
        (->> plan
             (keep (fn [[rs nodes]]
                     (when-not (= "config" rs)
                       (str "rs_" rs "/"
                            (first nodes) ":" client/shard-port))))))
      (->> plan
           (map (fn [[shard-name nodes]]
                  {:name  shard-name
                   :nodes nodes
                   :db    (replica-set-db)})))

      (db/tcpdump {:filter "host 192.168.122.1"
                   :ports  [client/mongos-port]}))))
