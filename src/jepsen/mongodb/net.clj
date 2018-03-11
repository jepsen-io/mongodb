(ns jepsen.mongodb.net
  "MongoDB specific controls for network manipulation."
  (:require [clj-time.core :as time]
            [jepsen.net :as net]
            [jepsen.util :refer [real-pmap]]
            [jepsen.mongodb.mongo :as m]
            [clojure.tools.logging :refer [info warn]])
  (:import (org.joda.time Period)))

(def ^:private clients
  "A cache for the MongoClients used by the `mongobridge` network manipulator."
  (atom nil))

(defn- create-clients
  "Given a collection `nodes`, returns a hash-map with keys equal to the nodes
  and values equals to a MongoClient instance to the associated node."
  [nodes]
  (into {} (map #(vector % (m/client %))) nodes))

(defn- get-or-create-clients
  "Given a collection `nodes`, returns a hash-map with keys equal to the nodes
  and values equals to a MongoClient instance to the associated node. This
  function guarantees that a MongoClient instance will be constructed exactly
  once for each node regardless of how many times it is called."
  [nodes]
  (while (nil? @clients)
    (compare-and-set! clients nil (delay (create-clients nodes))))
  (force @clients))

(defn- run-bridge-command
  [test node command-name & command-args]
  (let [client (get (get-or-create-clients (:nodes test)) node)]
    (try
      (info "running bridge command" node command-name command-args)
      (apply m/run-command!
             (m/db client "test")
             (concat `(~command-name 1 "$forBridge" true) command-args))
      (catch com.mongodb.MongoSocketException e
        (warn "bridge command failed likely due to node being killed"
              node command-name command-args (.getMessage e)))
      (catch com.mongodb.MongoTimeoutException e
        (warn "bridge command failed likely due to node being killed"
              node command-name command-args (.getMessage e))))))

(def mongobridge
  "Uses mongobridge to do network manipulation."
  (reify net/Net
    (drop! [net test src dest]
      (run-bridge-command test
                          dest
                          :rejectConnectionsFrom
                          :host (get (:bridge test) src)))

    ; The "delayMessagesFrom" command implicitly undoes the effects of an
    ; earlier "rejectConnectionsFrom" command by changing mongobridge's mode to
    ; forward the message with an optional delay.
    (heal! [net test] (net/fast! net test))

    (slow! [net test] (net/slow! net test {}))
    (slow! [net test {:keys [^Period mean], :or {mean (time/millis 50)}}]
      (->> (for [src  (vals (:bridge test))
                 dest (keys (:bridge test))]
             [src dest])
           (real-pmap (fn [[src dest]]
                        (run-bridge-command test
                                            dest
                                            :delayMessagesFrom
                                            :host src
                                            :delay (time/in-millis mean))))
           dorun))

    (flaky! [net test]
      (->> (for [src  (vals (:bridge test))
                 dest (keys (:bridge test))]
             [src dest])
           (real-pmap (fn [[src dest]]
                        (run-bridge-command test
                                            dest
                                            :discardMessagesFrom
                                            :host src
                                            :loss 0.2)))
           dorun))

    (fast! [net test] (net/slow! net test {:mean (time/millis 0)}))))
