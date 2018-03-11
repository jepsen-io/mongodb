(ns jepsen.mongodb.time
  "Controls time and clocks."
  (:require [clj-time.core :as time]
            [jepsen.nemesis.time :as nt])
  (:import (org.joda.time Period)))

(defprotocol Clock
  (init!
    [clock test]
    "Initializes any internal structures for time keeping on the current host.")

  (wrap!
    [clock test bin]
    "Returns how to invoke the specified binary under the control of this
    clock.")

  (bump-time!
    [clock test ^Period delta]
    "Advances the current host's clock by the specified time period.")

  (reset-time! [clock test]
               "Resets the current host's clock back to the true time."))

(defn noop-clock
  "Does nothing."
  [_]
  (reify Clock
    (init! [clock test])
    (wrap! [clock test bin] [bin])
    (bump-time! [clock test delta])
    (reset-time! [clock test])))

(defn system-clock
  "Changes the system's time using settimeofday()."
  [_]
  (reify Clock
    (init! [clock test])
    (wrap! [clock test bin] [bin])
    (bump-time! [clock test delta] (nt/bump-time! (time/in-millis delta)))
    (reset-time! [clock test] (nt/reset-time!))))
