(ns jepsen.mongodb.runner
  "Runs the full Mongo test suite, including a config file. Provides exit
  status reporting."
  (:gen-class)
  (:require [clojure.java.io :as io]
            [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as cli]
            [clojure.tools.logging :refer :all]
            [clojure.string :as str]
            [jepsen.mongodb [core :as m]
                            [document-cas :as dc]
                            [faketime :as faketime]
                            [mongo :as client]
                            [net :as mnet]
                            [read-concern-majority :as rcm]
                            [set :as set]
                            [time :as mt]
                            [sharded :as sharded]]
            [jepsen [cli :as jc]
                    [core :as jepsen]
                    [net :as net]
                    [os :as os]]
            [jepsen.os.debian :as debian]))

(def ^:private test-names
  {"set" set/test
   "register" dc/test
   "read-concern-majority" rcm/test
   "sharded-set" sharded/set-test
   "sharded-register" sharded/register-test})

(def ^:private clock-skew-mechs
  {"none" mt/noop-clock
   "systemtime" mt/system-clock
   "faketime" faketime/clock})

(def ^:private virt-mechs #{:none :lxc :vm})

(def ^:private virt-mech->clock-skew-mech
  {:none faketime/clock, :lxc faketime/clock, :vm mt/system-clock})

(def ^:private virt-mech->net-mech
  {:none mnet/mongobridge, :lxc net/iptables, :vm net/iptables})

(def ^:private virt-mech->os
  {:none os/noop, :lxc debian/os, :vm debian/os})

(defn- bridge->node+dest
  [offset node]
  (let [addr (client/server-address node)
        port (.getPort addr)]
    [node (str (client/server-address (.getHost addr) (+ port offset)))]))

(defn- process-opts
  [parsed]
  (let [{:keys [clock-skew
                libfaketime-path
                mongobridge-offset
                nodes
                ssh
                virtualization]}
        (:options parsed)]
    (assoc parsed :options
           (-> :options parsed
               (assoc :bridge (into {} (map (partial bridge->node+dest
                                                     mongobridge-offset))
                                       nodes)
                      :clock ((or clock-skew
                                  (get virt-mech->clock-skew-mech
                                       virtualization))
                              {:libfaketime-path libfaketime-path})
                      :net (get virt-mech->net-mech virtualization)
                      :os (get virt-mech->os virtualization)
                      :username "mongodb"
                      :virt virtualization)
               (cond->
                 (= :none virtualization) (assoc :ssh (assoc ssh :dummy? true))
                 ; Aside from sharing the same system clock, LXC containers
                 ; provide equivalent virtualization for our purposes (e.g. pid
                 ; namespaces, network namespaces) to that of virtual machine,
                 ; We therefore change to the :virt key to be :vm as a way to
                 ; avoid identical defmethods in other parts of our
                 ; cluster-management code.
                 (= :lxc virtualization) (assoc :virt :vm))
               (dissoc :clock-skew
                       :libfaketime-path
                       :mongobridge-offset
                       :virtualization)))))

(def ^:private opt-spec
  "Command line option specification for tools.cli."
  [[nil "--key-time-limit SECONDS"
    "How long should we test an individual key for, in seconds?"
    :default  100
    :parse-fn #(Long/parseLong %)
    :validate [pos? "Must be positive"]]

   ["-w" "--write-concern LEVEL" "Write concern level"
    :default  :majority
    :parse-fn keyword
    :validate [client/write-concerns (jc/one-of client/write-concerns)]]

   ["-r" "--read-concern LEVEL" "Read concern level"
    :default  :linearizable
    :parse-fn keyword
    :validate [client/read-concerns (jc/one-of client/read-concerns)]]

   [nil "--no-reads" "Disable reads, to test write safety only"]

   [nil "--read-with-find-and-modify" "Use findAndModify to ensure read safety"]

   ["-s" "--storage-engine ENGINE" "Mongod storage engine"
    :default  "wiredTiger"
    :validate [(partial re-find #"\A[a-zA-Z0-9]+\z") "Must be a single word"]]

   ["-p" "--protocol-version INT" "Replication protocol version number"
    :default  1
    :parse-fn #(Long/parseLong %)
    :validate [(complement neg?) "Must be non-negative"]]

   ["-t" "--test TEST-NAME" "Test to run"
    :default      set/test
    :default-desc "set"
    :parse-fn     test-names
    :validate     [identity (jc/one-of test-names)]]

   [nil "--working-dir DIR" "Directory for where to write MongoDB-related files"
    :default  "/opt/mongodb"
    :validate [#(.isAbsolute (io/file %)) "Must be an absolute path"]]

   [nil "--mongodb-dir DIR"
    (str "Directory to an existing checkout of the mongodb/mongo repository"
         " with binaries installed in the root directory. This option takes"
         " precedence over the --tarball option as the source of the MongoDB"
         " binaries to use.")
    :validate [#(.isDirectory (io/file %)) "Must be a directory"]]

   ["-z" "--virtualization MECH"
    "Mechanism for providing filesystem and network isolation"
    :default  :vm
    :parse-fn keyword
    :validate [(partial contains? virt-mechs) (jc/one-of virt-mechs)]]

   ["-c" "--clock-skew MECH" "Mechanism for performing clock skew"
    :default      nil
    :default-desc "systemtime if :vm and faketime otherwise"
    :parse-fn     clock-skew-mechs
    :validate     [identity (jc/one-of clock-skew-mechs)]]

   [nil "--libfaketime-path PATH"
    "Path to the libfaketime shared object to LD_PRELOAD"
    :default "/usr/lib/x86_64-linux-gnu/faketime/libfaketime.so.1"
    :validate [#(.isAbsolute (io/file %)) "Must be an absolute path"]]

   [nil "--mongobridge-offset INT"
    "Number of port values to stagger mongod processes ahead of mongobridge processes"
    :default  100
    :parse-fn #(Long/parseLong %)
    :validate [identity "Must be a number"]]

   [nil "--shard-count INT"
    "Number of shards. Defaults to 0."
    :default 0
    :parse-fn #(Long/parseLong %)
    :validate [identity "Must be a number"]]

   [nil "--chunk-size INT"
    "Size of shard chunks in INT MB."
    :default 64
    :parse-fn #(Long/parseLong %)
    :validate [identity "Must be a number"]]

   [nil "--mongos-count INT"
    "Number of mongos processes to run, max one per node"
    :parse-fn #(Long/parseLong %)
    :validate [identity "Must be a number"]]])

(defn -main
  [& args]
  (jc/run!
    (merge (jc/serve-cmd)
           (jc/single-test-cmd
             {:opt-spec opt-spec
              :opt-fn process-opts
              :tarball "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-debian81-3.4.0-rc3.tgz"
              :test-fn (fn [opts] ((:test opts) (dissoc opts :test)))}))
    args))
