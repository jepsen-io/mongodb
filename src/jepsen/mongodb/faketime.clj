(ns jepsen.mongodb.faketime
  "Functions for messing with time and clocks."
  (:require [clj-time.core :as time]
            [clojure.java.io :as io]
            [jepsen.control :as c]
            [jepsen.mongodb.time :as mt]
            [jepsen.mongodb.util :as mu])
  (:import (java.nio.charset Charset)
           (java.nio.file CopyOption)
           (java.nio.file Files)
           (java.nio.file OpenOption)
           (java.nio.file StandardCopyOption)
           (java.nio.file StandardOpenOption)
           (java.nio.file.attribute FileAttribute)
           (java.nio.file.attribute PosixFilePermissions)
           (org.joda.time Period)))

(defn- faketime-timestamp-file
  [test node]
  (mu/path-prefix test node "/faketimerc"))

(defn- upload-faketimerc!
  "Updates the faketimerc configuration file."
  [test ^Period offset]
  (let [dest     (faketime-timestamp-file test c/*host*)
        tmp-file (Files/createTempFile
                   "jepsen-upload"
                   ".faketimerc"
                   (into-array FileAttribute
                               [(PosixFilePermissions/asFileAttribute
                                  (PosixFilePermissions/fromString
                                    "rw-r--r--"))]))]
    (try
      (Files/write tmp-file
                   ; libfaketime requires a leading '+' for setting offsets
                   ; ahead of the true time.
                   [(format "%+d" (time/in-seconds offset))]
                   (Charset/defaultCharset)
                   (into-array OpenOption [StandardOpenOption/WRITE]))

      ; Swap the faketimerc configuration file such that its contents are
      ; atomically made visible to the underlying process.
      (if (= :vm (:virt test))
        ; If we're running with some form of virtualization, then we first
        ; upload the faketimerc configuration file to the remote host (via SCP)
        ; using its temporary name, and then atomically rename it to the
        ; FAKETIME_TIMESTAMP_FILE name.
        (let [tmp-file-dest
              (mu/path-prefix test c/*host* (str "/" (.getFileName tmp-file)))]
          (do (c/upload (str tmp-file) tmp-file-dest)
              (c/exec :mv :-f tmp-file-dest dest)))

        ; If we're running without any virtualization, then we just atomically
        ; rename it to the FAKETIME_TIMESTAMP_FILE name.
        (Files/move tmp-file
                    (.toPath (io/file dest))
                    (into-array CopyOption
                                [StandardCopyOption/ATOMIC_MOVE
                                 StandardCopyOption/REPLACE_EXISTING])))
      (finally
        (Files/deleteIfExists tmp-file)))))

(defn- add-periods [a b] (.plus (.toPeriod a) (.toPeriod b)))

(defn clock
  "Performs clock skewing using libfaketime and its file-based configuration
  mechanism."
  [{:keys [libfaketime-path]}]
  (let [state (atom {})
        true-time (time/seconds 0)]
    (reify mt/Clock
      (init! [clock test]
        (swap! state assoc c/*host* (atom true-time))
        ; We reset the faketimerc configuration file when initializing the clock
        ; to avoid causing heartbeat requests to time out spuriously when
        ; initiating the replica set.
        (upload-faketimerc! test true-time))

      (wrap! [clock test bin]
        (conj ["/usr/bin/env"
               "FAKETIME_NO_CACHE=1"
               (format "FAKETIME_TIMESTAMP_FILE=%s"
                       (faketime-timestamp-file test c/*host*))
               (format "LD_PRELOAD=%s" libfaketime-path)]
               bin))

      (bump-time! [clock test delta]
        (upload-faketimerc! test (swap! (get @state c/*host*)
                                        (partial add-periods delta))))

      (reset-time! [clock test]
        (upload-faketimerc! test (swap! (get @state c/*host*)
                                        (constantly true-time)))))))
