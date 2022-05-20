(ns jepsen.mongodb.db.proxy
  "Sets up a proxy binary on each DB node."
  (:require [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh random-nonempty-subset sh]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb [client :as client :refer [Conn
                                                       host
                                                       port]]]
            [slingshot.slingshot :refer [try+ throw+]]))

(def dir
  "Where do we install our proxy?"
  "/opt/jepsen/mongo-proxy")

(def log-file (str dir "/log"))
(def pid-file (str dir "/pid"))
(def bin      (str dir "/mongo-proxy"))

(def listener-count
  "How many proxy listener ports do we bind?"
  3)

(defn start!
  "Starts the proxy for a test."
  [test db node]
  (c/su
    (cu/start-daemon!
      {:logfile log-file
       :pidfile pid-file
       :chdir   dir}
      bin
      :-mongo-upstream (str (cn/ip node) ":" (port db test))
      :-bind-address   (cn/ip node)
      :-listener-count listener-count)))

(defrecord ProxyDB [db]
  Conn
  (host [_ test node]
    (host db test node))

  (port [_ test]
    (+ client/proxy-port (rand-int listener-count)))

  db/DB
  (setup! [this test node]
    (db/setup! db test node)
    (when-let [proxy (:proxy test)]
      (c/su
        (c/exec :mkdir :-p dir)
        (c/upload proxy bin)
        (c/exec :chmod :+x bin)
        (start! test db node))))

  (teardown! [this test node]
    (when (:proxy test)
      (c/su (cu/stop-daemon! bin pid-file))
            (c/exec :rm :-rf pid-file log-file))
    (db/teardown! db test node))

  db/Primary
  (setup-primary! [_ test node]
    (db/setup-primary! db test node))

  (primaries [_ test]
    (db/primaries db test))

  db/LogFiles
  (log-files [_ test node]
    (merge (db/log-files db test node)
           (when (:proxy test)
             {log-file "proxy.log"})))

  ; We don't bother injecting faults into the proxy itself
  db/Process
  (start! [_ test node]
    (db/start! db test node)
    )

  (kill! [_ test node]
    (db/kill! db test node))

  db/Pause
  (pause! [_ test node]
    (db/pause! db test node))

  (resume! [this test node]
    (db/resume! db test node)))

(defn db
  "Wraps another database, uploading a local proxy binary (:proxy test)
  to each node, and running it there."
  [db]
  (ProxyDB. db))
