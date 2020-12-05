(ns jepsen.mongodb.nemesis
  "Nemeses for MongoDB"
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [nemesis :as n]
                    [net :as net]
                    [util :as util]]
            [jepsen.generator :as gen]
            [jepsen.nemesis [combined :as nc]
                            [time :as nt]]
            [jepsen.mongodb.db :as db]))

(defn shard-generator
  "Takes a collection of shard packages, and returns a generator that emits ops
  like {:f whatever, :shard \"foo\", :value blah}, drawn from one of the shard
  packages."
  [packages]
  (->> packages
       (map (fn [pkg]
              (let [shard-name (:name pkg)]
                (gen/map (fn [op] (assoc op :shard shard-name))
                         (:generator pkg)))))
       gen/mix))

(defn shard-nemesis
  "Takes a collection of shard packages, and returns a nemesis that
  takes ops like {:f whatever, :shard \"foo\", :value blah}, and dispatches
  that op to the nemesis for that particular shard."
  [packages]
  (reify n/Nemesis
    (setup! [this test]
      (real-pmap (fn [pkg]
                   (n/setup! (:nemesis pkg)
                             (db/test-for-shard test pkg)))
                 packages))

    (invoke! [this test op]
      (let [shard-name (:shard op)
            pkg        (first (filter (comp #{shard-name} :name) packages))
            nemesis    (:nemesis pkg)
            test       (db/test-for-shard test pkg)
            op'        (n/invoke! (:nemesis pkg) test op)]
        op'))

    (teardown! [this test]
      (real-pmap (fn [pkg]
                   (n/teardown! (:nemesis pkg)
                                (db/test-for-shard test pkg)))
                 packages))

    n/Reflection
    (fs [this]
      (set (mapcat (comp n/fs :nemesis) packages)))))

(defn package-for-shard
  "Builds a nemesis package for a specific shard, merged with the shard map
  itself."
  [opts shard]
  (merge shard
         (nc/nemesis-package (assoc opts :db (:db shard)))))

(defn nemesis-package
  "Constructs a nemesis and generators for MongoDB."
  [opts]
  (let [opts (update opts :faults set)
        ; Construct a package for each shard
        pkgs (map (partial package-for-shard opts) (:shards (:db opts)))]

    ; Now, we need a generator and nemesis which mix operations on various
    ; shards, and route those operations to the nemesis for each appropriate
    ; shards. We merge these onto a nemesis package for the whole test--that
    ; gives us
    (assoc (nc/nemesis-package opts)
           :generator        (shard-generator pkgs)
           :final-generator  nil
           :nemesis          (shard-nemesis pkgs)))
    ; Or just do a standard package
    ; TODO: mix these
    ;(nc/nemesis-package)
    )
