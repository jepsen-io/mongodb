(ns jepsen.mongodb.nemesis
  "Nemeses for MongoDB"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.generator.pure :as gen]
            [jepsen.nemesis [combined :as nc]
                            [time :as nt]]
            [jepsen.mongodb.db :as db]))

(defn nemesis-package
  "Constructs a nemesis and generators for MongoDB."
  [opts]
  (let [opts (update opts :faults set)]
    (info :nemesis opts)
    (-> (nc/nemesis-packages opts)
        (->> (remove nil?))
        nc/compose-packages)))
