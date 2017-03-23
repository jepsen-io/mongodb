(ns jepsen.mongodb.faketime
  "Functions for messing with time and clocks."
  (:require [clj-time.core :as time]
            [jepsen.control :as c]
            [jepsen.mongodb.time :as mt])
  (:import (java.nio.charset Charset)
           (java.nio.file Files)
           (java.nio.file OpenOption)
           (java.nio.file StandardOpenOption)
           (java.nio.file.attribute FileAttribute)
           (java.nio.file.attribute PosixFilePermissions)
           (org.joda.time Period)))

(defn- upload-faketimerc!
  "Updates the /opt/jepsen/faketimerc configuration file."
  [^Period offset]
  (c/su
    (let [tmp-file
          (Files/createTempFile
            "jepsen-upload"
            ".faketimerc"
            (into-array FileAttribute [(PosixFilePermissions/asFileAttribute
                                         (PosixFilePermissions/fromString
                                           "rw-r--r--"))]))]
      (try
        (Files/write tmp-file
                     ; libfaketime requires a leading '+' for setting offsets
                     ; ahead of the true time.
                     [(format "%+d" (time/in-seconds offset))]
                     (Charset/defaultCharset)
                     (into-array OpenOption [StandardOpenOption/WRITE]))
        ; Upload
        (c/exec :mkdir :-p "/opt/jepsen")
        (c/exec :chmod "a+rwx" "/opt/jepsen")
        (c/upload (str tmp-file) "/opt/jepsen/faketimerc")
        (finally
          (Files/delete tmp-file))))))

(defn- add-periods [a b] (.plus (.toPeriod a) (.toPeriod b)))

(defn clock
  "Performs clock skewing using libfaketime and its file-based configuration
  mechanism."
  ([{:keys [libfaketime-path]}]
   (let [state (atom {})
         true-time (time/seconds 0)]
     (reify mt/Clock
       (init! [clock]
         (swap! state assoc c/*host* (atom true-time))
         ; We reset the faketimerc configuration file when initializing the clock
         ; to avoid causing heartbeat requests to time out spuriously when
         ; initiating the replica set.
         (upload-faketimerc! true-time))

       (wrap! [clock bin]
         (conj ["/usr/bin/env"
                "FAKETIME_NO_CACHE=1"
                "FAKETIME_TIMESTAMP_FILE=/opt/jepsen/faketimerc"
                (format "LD_PRELOAD=%s" libfaketime-path)]
                bin))

       (bump-time! [clock delta]
         (upload-faketimerc! (swap! (get @state c/*host*)
                                    (partial add-periods delta))))

       (reset-time! [clock]
         (upload-faketimerc! (swap! (get @state c/*host*)
                                    (constantly true-time))))))))
