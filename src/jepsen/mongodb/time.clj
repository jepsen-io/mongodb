(ns jepsen.mongodb.time
  "Controls time and clocks."
  (:require [clj-time.core :as time]
            [jepsen.nemesis.time :as nt])
  (:import (org.joda.time Period)))

(defprotocol Clock
  (init!
    [clock]
    "Initializes any internal structures for time keeping on the current host.")

  (wrap!
    [clock bin]
    "Returns how to invoke the specified binary under the control of this
    clock.")

  (bump-time!
    [clock ^Period delta]
    "Advances the current host's clock by the specified time period.")

  (reset-time! [clock]
               "Resets the current host's clock back to the true time."))

(defn noop-clock
  "Does nothing."
  ([_]
   (reify Clock
     (init! [clock])
     (wrap! [clock bin] [bin])
     (bump-time! [clock delta])
     (reset-time! [clock]))))

(defn system-clock
  "Changes the system's time using settimeofday()."
  ([_]
   (reify Clock
     (init! [clock])
     (wrap! [clock bin] [bin])
     (bump-time! [clock delta] (nt/bump-time! (time/in-millis delta)))
     (reset-time! [clock] (nt/reset-time!)))))
