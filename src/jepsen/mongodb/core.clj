(ns jepsen.mongodb.core
  (:require [clojure [pprint :refer :all]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [debug info warn]]
            [clojure.walk :as walk]
            [clj-time.core :as time]
            [jepsen [core      :as jepsen]
                    [db        :as db]
                    [util      :as util :refer [meh
                                                real-pmap
                                                with-thread-name
                                                fcatch
                                                exception?
                                                random-nonempty-subset
                                                timeout]]
                    [control   :as c :refer [|]]
                    [client    :as client]
                    [checker   :as checker]
                    [generator :as gen]
                    [nemesis   :as nemesis]
                    [net       :as net]
                    [store     :as store]
                    [report    :as report]
                    [tests     :as tests]]
            [jepsen.nemesis.time :as nt]
            [jepsen.control [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb.cluster :as mc]
            [jepsen.mongodb.control :as mcontrol]
            [jepsen.mongodb.dbhash :as dbhash]
            [jepsen.mongodb.dbutil :as mdbutil]
            [jepsen.mongodb.mongo :as m]
            [jepsen.mongodb.net :as mnet]
            [jepsen.mongodb.time :as mt]
            [jepsen.mongodb.util :as mu]
            [knossos [core :as knossos]
                     [model :as model]])
  (:import (clojure.lang ExceptionInfo)))

(defn mongo!
  "Run a Mongo shell command. Spits back an unparsable kinda-json string,
  because what else would 'printjson' do?"
  [cmd]
  (-> (c/exec :mongo :--quiet :--eval (str "printjson(" cmd ")"))))

;; Cluster setup

(defn replica-set-status
  "Returns the current replica set status."
  [conn]
  (m/admin-command! conn :replSetGetStatus 1))

(defn replica-set-initiate!
  "Initialize a replica set on a node."
  [conn config]
  (try
    (m/admin-command! conn :replSetInitiate config)
    (catch ExceptionInfo e
      (condp re-find (get-in (ex-data e) [:result "errmsg"])
        ; Some of the time (but not all the time; why?) Mongo returns this error
        ; from replsetinitiate, which is, as far as I can tell, not actually an
        ; error (?)
        #"Received replSetInitiate - should come online shortly"
        nil

        ; This is a hint we should back off and retry; one of the nodes probably
        ; isn't fully alive yet.
        #"need all members up to initiate, not ok"
        (do (info "not all members alive yet; retrying replica set initiate"
            (Thread/sleep 1000)
            (replica-set-initiate! conn config)))

        ; Or by default re-throw
        (throw e)))))

(defn replica-set-master?
  "What's this node's replset role?"
  [conn]
  (m/admin-command! conn :isMaster 1))

(defn replica-set-config
  "Returns the current replset config."
  [conn]
  (m/admin-command! conn :replSetGetConfig 1))

(defn replica-set-reconfigure!
  "Apply new configuration for a replica set."
  [conn conf]
  (m/admin-command! conn :replSetReconfig conf))

(defn primaries
  "What nodes does this conn think are primaries?"
  [conn]
  (->> (replica-set-status conn)
       :members
       (filter #(= "PRIMARY" (:stateStr %)))
       (map :name)
       (map m/server-address)))

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

(defn await-conn
  "Block until we can connect to the given node. Returns a connection to the
  node."
  [node]
  (timeout (* 300 1000)
           (throw (ex-info "Timed out trying to connect to MongoDB"
                           {:node node}))
           (loop []
             (or (try
                   (let [conn (m/client node)]
                     (try
                       (.first (.listDatabaseNames conn))
                       conn
                       ; Don't leak clients when they fail
                       (catch Throwable t
                         (.close conn)
                         (throw t))))
                   (catch com.mongodb.MongoTimeoutException e
                     (info "Mongo timeout while waiting for conn; retrying. "
                           (.getMessage e))
                     nil)
                   (catch com.mongodb.MongoSocketReadTimeoutException e
                     (info "Mongo socket read timeout waiting for conn; retrying")
                     nil))
                 ; If we aren't ready, sleep and retry
                 (do
                   (Thread/sleep 1000)
                   (recur))))))

(defn await-primary
  "Block until a primary is known to the current node."
  [conn]
  (while (not (primary conn))
    (Thread/sleep 1000)))

(defn await-join
  "Block until all nodes in the test are known to this connection's replset
  status"
  [test conn]
  (while (try (not= (set (map m/server-address (:nodes test)))
                    (->> (replica-set-status conn)
                         :members
                         (map :name)
                         (map m/server-address)
                         set))
              (catch ExceptionInfo e
                (if (re-find #"should come online shortly"
                             (get-in (ex-data e) [:result "errmsg"]))
                  true
                  (throw e))))
    (info :replica-set-status (with-out-str (->> (replica-set-status conn)
                                                 :members
                                                 (map :name)
                                                 (map m/server-address)
                                                 (map str)
                                                 pprint)))
    (Thread/sleep 1000)))

(defn target-replica-set-config
  "Generates the config for a replset in a given test."
  [test]
  (assert (integer? (:protocol-version test)))
  {:_id "jepsen"
   :protocolVersion (:protocol-version test)
   :settings {:heartbeatIntervalMillis 2500  ; protocol v1, ms
              :electionTimeoutMillis   5000 ; protocol v1, ms
              :heartbeatTimeoutSecs    5}  ; protocol v0, s
   :members (->> test
                 :nodes
                 (map-indexed (fn [i node]
                                {:_id  i
                                 :priority (- (count (:nodes test)) i)
                                 :host (str (m/server-address node))})))})

(defn join!
  "Join nodes into a replica set. Blocks until any primary is visible to all
  nodes which isn't really what we want but oh well."
  [test node]
  ; Gotta have all nodes online for this. Delightfully, Mongo won't actually
  ; bind to the port until well *after* the init script startup process
  ; returns. This would be fine, except that  if a node isn't ready to join,
  ; the initiating node will just hang indefinitely, instead of figuring out
  ; that the node came online a few seconds later.
  (.close (await-conn node))
  (jepsen/synchronize test)

  ; Initiate RS
  (when (= node (jepsen/primary test))
    (with-open [conn (await-conn node)]
      (info node "Initiating replica set")
      (replica-set-initiate! conn (target-replica-set-config test))

      (info node "Jepsen primary waiting for cluster join")
      (await-join test conn)
      (info node "Jepsen primary waiting for mongo election")
      (await-primary conn)
      (info node "Primary ready.")))

  ; For reasons I really don't understand, you have to prevent other nodes
  ; from checking the replset status until *after* we initiate the replset on
  ; the primary--so we insert a barrier here to make sure other nodes don't
  ; wait until primary initiation is complete.
  (jepsen/synchronize test)

  ; For other reasons I don't understand, you *have* to open a new set of
  ; connections after replset initation. I have a hunch that this happens
  ; because of a deadlock or something in mongodb itself, but it could also
  ; be a client connection-closing-detection bug.

  ; Amusingly, we can't just time out these operations; the client appears to
  ; swallow thread interrupts and keep on doing, well, something. FML.
  (with-open [conn (await-conn node)]
    (info node "waiting for cluster join")
    (await-join test conn)

    (info node "waiting for primary")
    (await-primary conn)

    (info node "primary is" (str (primary conn)))
    (jepsen/synchronize test)))

(defn db
  "MongoDB for a particular HTTP URL"
  [clock url]
  (let [state (atom {})]
    ; We intentionally do not implement the jepsen.db/LogFiles protocol in order
    ; to avoid the jepsen.core/snarf-logs! function from being called when an
    ; exception occurs or when `db` is being torn down. The
    ; jepsen.core/snarf-logs! function uses the jepsen.control/download
    ; function, which requires the use of an SSH connection and is therefore
    ; incompatible with the concept of running without any virtualization. The
    ; teardown! method is instead responsible for downloading/copying the logs
    ; to the store/ directory.
    (reify db/DB
      (setup! [_ test node]
        (swap! state assoc node {:setup-called true})
        (util/timeout 300000
                      (throw (RuntimeException.
                               (str "Mongo setup on " node " timed out!")))
                      (when (= :vm (:virt test))
                        (debian/install [:libc++1 :libsnmp30]))

                      ; The jepsen.mongodb.dbutil/install! function creates the
                      ; subdirectories that the jepsen.mongodb.time/init!
                      ; function and the jepsen.mongodb.cluster/init! function
                      ; attempt to create files in, so it must happen first.
                      (->> (or (some->> (:mongodb-dir test)
                                        io/file
                                        .getCanonicalPath
                                        (str "file://"))
                               url)
                           (mdbutil/install! test node))

                      (mt/init! clock test)
                      (mc/init! test node)
                      (mc/start! clock test node)
                      (join! test node)))

      (teardown! [_ test node]
        ; We detect when jepsen.db/cycle! is being called in jepsen.core/run! as
        ; a case when db/teardown! is called while db/setup! has yet to be
        ; called. We forcibly terminate any mongod processes that may be running
        ; in order to prevent hangs from an earlier execution from causing
        ; additional failures.
        (if-not (:setup-called (get @state node))
          (mc/kill! test node)
          ; We call the snarf-logs! function before stopping the node to match
          ; the behavior of how jepsen.core/snarf-logs! is called before
          ; jepsen.db/teardown! is called. This is useful for gathering at least
          ; partial logs if mongod is going to hang during clean shutdown.
          (do (mdbutil/snarf-logs! test node)
              (try
                (mc/stop! test node)
                ; We call the snarf-logs! function after stopping the node to
                ; ensure the shutdown messages from mongod are included in the
                ; downloaded logs.
                (finally (mdbutil/snarf-logs! test node)))))))))

(defmacro with-errors
  "Takes an invocation operation, a set of idempotent operation functions which
  can be safely assumed to fail without altering the model state, and a body to
  evaluate. Catches MongoDB errors and maps them to failure ops matching the
  invocation."
  [op idempotent-ops & body]
  `(let [error-type# (if (~idempotent-ops (:f ~op))
                       :fail
                       :info)]
     (try
       ~@body
       (catch com.mongodb.MongoQueryException e#
         (case (.getCode e#)
           189   (assoc ~op :type :fail, :error :stepped-down)
           10107 (assoc ~op :type :fail, :error :not-primary)
           (throw e#)))

       (catch com.mongodb.MongoNotPrimaryException e#
         (assoc ~op :type :fail, :error :not-primary))

       (catch com.mongodb.MongoTimeoutException e#
         (condp re-find (.getMessage e#)
           #"Timed out .+? while waiting for a server that matches .+?ServerSelector"
           (assoc ~op :type :fail, :error :no-ready-server)
           (throw e#)))

       ; A network error is indeterminate
       (catch com.mongodb.MongoSocketReadException e#
         (assoc ~op :type error-type# :error :socket-read))

       (catch com.mongodb.MongoSocketReadTimeoutException e#
         (assoc ~op :type error-type# :error :socket-read)))))

(defn kill-nem
  "A nemesis that kills/restarts Mongo on randomly selected nodes."
  [clock]
  (nemesis/node-start-stopper random-nonempty-subset
                              mc/kill!
                              (partial mc/start! clock)))

(defn pause-nem
  "A nemesis that pauses Mongo on randomly selected nodes."
  []
  (nemesis/hammer-time random-nonempty-subset "mongod"))

(defn clock-skew-nem
  "Skews clocks on a random subset of nodes by dt seconds."
  [clock dt]
  (reify client/Client
    (setup! [this test _]
      (c/with-test-nodes test (mt/reset-time! clock test))
      this)

    (invoke! [this test op]
      (assoc op :value
             (case (:f op)
               :start (c/with-test-nodes test
                        (if (< (rand) 0.5)
                          (do (mt/bump-time! clock test (time/seconds dt))
                              dt)
                          0))
               :stop (info c/*host* "clock reset:" (mt/reset-time! clock test)))))

    (teardown! [this test]
      (c/with-test-nodes test (mt/reset-time! clock test)))))

(defn nemesis-gen
  "Given a nemesis name, builds a generator that emits [:name-start,
  :name-stop] cycles."
  [nem]
  (gen/seq
    (cycle
      [{:type :info, :f (keyword (str (name nem) "-start")), :value nil}
       {:type :info, :f (keyword (str (name nem) "-stop" )), :value nil}])))

(defn std-gen
  "A composite failure schedule, emitting partitions and clock skew ops."
  []
  (->> [:part :skew] ; :kill :pause]
       (mapv nemesis-gen)
       (gen/mix)
       (gen/delay 5)))

(defn composite-nemesis
  "Combined nemesis for process kills, pauses, partitions, and clock skew."
  []
  (nemesis/compose
    {{:part-start  :start, :part-stop  :stop}
     (nemesis/partition-majorities-ring)
     {:skew-start  :start, :skew-stop  :stop} (clock-skew-nem 256)}))
;     {:kill-start  :start, :kill-stop  :stop} (kill-nem)
;     {:pause-start :start, :pause-stop :stop} (pause-nem)}))


(defn primary-divergence-nemesis
  "A nemesis specifically designed to break Mongo's v0 replication protocol.
  There are three phases:

  1. Isolate a primary p1 and advance its clock. Writes to this primary will
  not be successfully replicated, but will advance its oplog.

  2. Allow a new primary p2 to become elected. Let it do some work, then kill
  it.

  3. Heal the network and restart all nodes. p2 may have committed writes, but
  p1's higher optime will allow it to win the election."
  ([clock] (primary-divergence-nemesis clock nil))
  ([clock conns]
  (reify client/Client
    (setup! [this test _]
      (primary-divergence-nemesis
        clock
        (into {} (real-pmap (juxt identity await-conn) (:nodes test)))))

    (invoke! [this test op]
      (assoc
        op :value
        (case (:f op)
          :isolate (->> conns
                        (real-pmap (fn [[node conn]]
                          (when (= (m/server-address node) (primary conn))
                            (info node "believes itself a primary")
                            (->> (nemesis/split-one node (:nodes test))
                                 nemesis/complete-grudge
                                 (net/drop-all! test))
                            (info node "isolated")

                            (c/with-session node (get (:sessions test) node)
                              (mt/bump-time! clock test (time/minutes 2)))
                            (info node "clock advanced"))))
                        dorun)

          :kill (->> conns
                     (real-pmap (fn [[node conn]]
                       (when (= (m/server-address node) (primary conn))
                         (info node "believes itself a primary")
                         (c/with-session node (get (:sessions test) node)
                           (mc/kill! test node))
                         (info node "mongod killed"))))
                     dorun)

          :stop (do (c/with-test-nodes test (mt/reset-time! clock test))
                    (info "Clocks reset")
                    (net/heal! (:net test) test)
                    (info "Network healed")
                    (c/on-nodes test (partial mc/start! clock))
                    (info "Nodes restarted")))))

    (teardown! [this test]
               (doseq [[node c] conns]
                 (.close c))))))

(defn primary-divergence-gen
  []
  (gen/seq
    (cycle [{:type :info, :f :isolate, :value nil}
            (gen/sleep 30)
            {:type :info, :f :kill,    :value nil}
            {:type :info, :f :stop,    :value nil}
            (gen/sleep 30)])))

(defn test-
  "Constructs a test with the given name prefixed by 'mongodb ', merging any
  given options. Special options for Mongo:

  :tarball            HTTP URL of a tarball to install
  :time-limit         How long do we run the test for?
  :storage-engine     Storage engine to use
  :protocol-version   Replication protocol version

  At the end of every test, we use the 'dbHash' command to verify data
  consistency across each member of the replica set. We extend the nemesis to
  handle the :compare-dbhashes operations because it is a client that is
  straightforward to compose via the `jepsen.nemesis/compose` function. We also
  extend the generator and checker accordingly."
  [name opts]
  (merge
    (assoc tests/noop-test
           :name            (str "mongodb " name " s:" (:storage-engine opts)
                                 " p:" (:protocol-version opts))
           :os              debian/os
           :db              (db (:clock opts) (:tarball opts))
           :nemesis         (nemesis/compose
                              ; We allow for dbhash checks by composing our
                              ; standard "chaos" nemesis with a client that
                              ; handles :compare-dbhashes ops.
                              {#{:isolate :kill :stop}
                               (primary-divergence-nemesis (:clock opts))
                               #{:compare-dbhashes} dbhash/client})
           :generator       (gen/phases
                              (->> (:generator opts)
                                   (gen/nemesis (primary-divergence-gen))
                                   (gen/time-limit (:time-limit opts)))
                              (gen/nemesis
                                (gen/once {:type :info, :f :stop, :value nil}))
                              (gen/sleep 40)
                              (gen/clients (:final-generator opts))
                              ; Generate the :compare-dbhashes op at the very
                              ; end of the test.
                              (gen/nemesis dbhash/gen))
           :client            (:client opts)
           :checker           (checker/compose
                                {:base (:checker opts)
                                 :dbhash dbhash/checker}))
    (dissoc opts
            :checker
            :client
            :generator
            :final-generator
            :clock)))
