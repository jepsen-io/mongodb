(ns jepsen.mongodb.cluster
  "Functions for starting and stopping MongoDB deployments."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.mongodb.control :as mcontrol]
            [jepsen.mongodb.mongo :as m]
            [jepsen.mongodb.time :as mt]
            [jepsen.mongodb.util :as mu]))

;; Copied from the jepsen.control.util/start-daemon! function of
;; Jepsen version 0.1.8 with the `exec` function calls replaced with
;; `mcontrol/exec` function calls to account for the possibility of running
;; without any virtualization.
(defn- start-daemon!
  "Starts a daemon process, logging stdout and stderr to the given file.
  Invokes `bin` with `args`. Options are:

  :background?
  :chdir
  :logfile
  :make-pidfile?
  :match-executable?
  :match-process-name?
  :pidfile
  :process-name"
  [test opts bin & args]
  (info "starting" (.getName (io/file (name bin))))
  (mcontrol/exec test
                 :echo (c/lit "`date +'%Y-%m-%d %H:%M:%S'`")
                 "Jepsen starting" bin (str/join " " args)
                 :>> (:logfile opts))
  (apply mcontrol/exec test
         :start-stop-daemon :--start
         (when (:background? opts true) [:--background :--no-close])
         (when (:make-pidfile? opts true) :--make-pidfile)
         (when (:match-executable? opts true) [:--exec bin])
         (when (:match-process-name? opts false)
           [:--name (:process-name opts (.getName (io/file bin)))])
         :--pidfile  (:pidfile opts)
         :--chdir    (:chdir opts)
         :--oknodo
         :--startas  bin
         :--
         (concat args [:>> (:logfile opts) (c/lit "2>&1")])))

;; The jepsen.control.util/stop-daemon! function uses the kill -9 command to
;; terminate the process associated with the pidfile. We would rather send a
;; SIGTERM to the process in order to be able to detect hangs the may occur
;; during a clean shutdown.
(defn- stop-daemon!
  "Sends a daemon process identified by its pidfile a SIGTERM and waits until
  the process exits."
  [test pidfile]
  (info "stopping" pidfile)
  (mcontrol/exec test
                 :start-stop-daemon :--stop
                 :--pidfile pidfile
                 ; We wait in increments of 60 seconds to avoid busy-waiting if
                 ; the process is actually hung.
                 :--retry "TERM/forever/60"
                 :--oknodo))

(defn- kill-daemon!
  "Sends a daemon process identified by its pidfile a SIGKILL and waits until
  the process exits."
  [test pidfile]
  (info "killing" pidfile)
  (mcontrol/exec test
                 :start-stop-daemon :--stop
                 :--pidfile pidfile
                 :--retry "KILL/forever/0"
                 :--oknodo))

;;
;; init! methods
;;

(defmulti init! (fn [test node] (:virt test)))

(defn- write-config-file!
  [test node]
  (mcontrol/exec test
    :echo (-> "mongod.conf" io/resource slurp
              (str/replace #"%ENABLE_MAJORITY_READ_CONCERN%"
                           (str (= (:read-concern test) :majority)))
              (str/replace #"%PATH_PREFIX%" (mu/path-prefix test node))
              (str/replace #"%STORAGE_ENGINE%" (:storage-engine test)))
    :> (mu/path-prefix test node "/mongod.conf")))

(defmethod init! :none [test node]
  (write-config-file! test node))

(defmethod init! :vm [test node]
  (c/sudo (:username test) (write-config-file! test node)))

;;
;; start! methods
;;

(defmulti start! (fn [clock test node] (:virt test)))

(defn- start-mongod!
  [clock test node port]
  (apply start-daemon! test
         {:chdir (mu/path-prefix test node)
          :background? false
          :logfile (mu/path-prefix test node "/stdout.log")
          :make-pidfile? false
          :match-executable? false
          :match-process-name? true
          :pidfile (mu/path-prefix test node "/mongod.pid")
          :process-name "mongod"}
         (conj (mt/wrap! clock test (mu/path-prefix test node "/bin/mongod"))
               :--fork
               :--pidfilepath (mu/path-prefix test node "/mongod.pid")
               :--port port
               :--config (mu/path-prefix test node "/mongod.conf"))))

(defn- start-mongobridge!
  [test node dest]
  (start-daemon! test {:chdir (mu/path-prefix test node)
                       :background? true
                       :logfile (mu/path-prefix test node "/bridge.log")
                       :make-pidfile? true
                       :match-executable? false
                       :match-process-name? true
                       :pidfile (mu/path-prefix test node "/bridge.pid")
                       :process-name "mongobridge"}
                      (mu/path-prefix test node "/bin/mongobridge")
                      :--port (.getPort (m/server-address node))
                      :--dest dest
                      :--verbose))

(defmethod start! :none [clock test node]
  (let [dest (get (:bridge test) node)]
    (start-mongobridge! test node dest)
    (start-mongod! clock test node (.getPort (m/server-address dest)))
    :started))

(defmethod start! :vm [clock test node]
  (c/sudo (:username test)
          (start-mongod! clock test node (.getPort (m/server-address node))))
  :started)

;;
;; stop! methods
;;

(defmulti stop! (fn [test node] (:virt test)))

(defn- stop-mongod!
  [test node]
  (stop-daemon! test (mu/path-prefix test node "/mongod.pid")))

(defn- stop-mongobridge!
  [test node]
  (stop-daemon! test (mu/path-prefix test node "/bridge.pid")))

(defmethod stop! :none [test node]
  (stop-mongod! test node)
  (stop-mongobridge! test node)
  :stopped)

(defmethod stop! :vm [test node]
  (c/sudo (:username test) (stop-mongod! test node))
  :stopped)

;;
;; kill! methods
;;

(defmulti kill! (fn [test node] (:virt test)))

(defn- kill-mongod!
  [test node]
  (kill-daemon! test (mu/path-prefix test node "/mongod.pid")))

(defmethod kill! :none [test node]
  (kill-mongod! test node)
  ; mongobridge doesn't have any durable state so there isn't anything to be
  ; gained from killing it. We ask it nicely to shut down instead.
  (stop-mongobridge! test node)
  :stopped)

(defmethod kill! :vm [test node]
  (c/sudo (:username test) (kill-mongod! test node))
  :stopped)
