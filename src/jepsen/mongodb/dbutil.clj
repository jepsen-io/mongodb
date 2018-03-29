(ns jepsen.mongodb.dbutil
  "Utility functions for initializing and finalizing a MongoDB database's
  environment."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.store :as store]
            [jepsen.util :as util]
            [jepsen.mongodb.control :as mcontrol]
            [jepsen.mongodb.util :as mu])
  (:import (java.nio.file CopyOption)
           (java.nio.file Files)
           (java.nio.file StandardCopyOption)))

(defn- upload!
  "Uploads the `local` file to the `remote` file location."
  [test [local remote]]
  (info "uploading" local "to" remote)
  (if (= :vm (:virt test))
    (c/upload local remote :mode 0755)
    (Files/copy (.toPath (io/file local))
                (.toPath (io/file remote))
                (into-array CopyOption
                            [StandardCopyOption/COPY_ATTRIBUTES
                             StandardCopyOption/REPLACE_EXISTING]))))

(defn- install-local-directory!
  "Uploads MongoDB binaries from a local directory."
  [test node dir]
  ; Create the bin/ directory.
  (mcontrol/exec test :mkdir :-p (mu/path-prefix test node "/bin"))

  ; Upload the mongod and mongo shell binaries. The mongobridge binary is also
  ; uploaded if we aren't running with any virtualization.
  (->> (cond-> [[(str dir "/mongod")
                 (mu/path-prefix test node "/bin/mongod")]
                [(str dir "/mongo")
                 (mu/path-prefix test node "/bin/mongo")]]
         (= :none (:virt test))
         (conj [(str dir "/mongobridge")
                (mu/path-prefix test node "/bin/mongobridge")]))
       (map (partial upload! test))
       dorun))

(defn- install-local-tarball!
  "Extracts MongoDB binaries from an already present tarball."
  [test node file]
  ; Extract the tarball into the /opt/mongodb/ directory.
  (c/exec :tar :xvf file :-C (mu/path-prefix test node)
          :--strip-components=1))

; The install-remote-tarball! is mutually recursive with the install-binaries!
; function so we need to forward declare one of them.
(declare install-binaries!)

(defn- install-remote-tarball!
  "Downloads and extracts MongoDB binaries from a remote URL."
  [test node url]
  (when (= :none (:virt test))
    (throw (IllegalStateException.
             (str "The jepsen.control.util/wget! function cannot be used to"
                  " download a tarball locally. Please consider using the"
                  " --mongodb-dir command line option to refer to an existing"
                  " checkout of the mongodb/mongo repository with binaries"
                  " installed in the root directory."))))

  (let [file (c/cd "/tmp" (str "/tmp/" (cu/wget! url)))]
    (try
      ; Extract the tarball into the /opt/mongodb/ directory.
      (c/exec :tar :xvf file :-C (mu/path-prefix test node)
              :--strip-components=1)

      (catch RuntimeException e
        (condp re-find (.getMessage e)
          #"tar: Unexpected EOF"
          (do (info "Retrying corrupt tarball download")
              (c/exec :rm :-rf file)
              (install-binaries! test node url))

          ; If it was some other kind of error, then we rethrow it.
          (throw e))))))

(defn- install-binaries!
  "Installs the MongoDB binaries into the node's directory from a local
  directory, a local tarball, or a remote tarball."
  [test node url]
  ;; Clean up the node's directory from a prior execution of the test.
  (mcontrol/exec test :rm :-rf (mu/path-prefix test node))

  ;; Create the data/ directory.
  (mcontrol/exec test :mkdir :-p (mu/path-prefix test node "/data"))

  ;; Create the configsvr-data/ directories for sharded tests.
  (when (< 0 (:shard-count test))
    (mcontrol/exec test :mkdir :-p (mu/path-prefix test node "/configsvr-data")))

  ;; Create data directories for each shard. If `shard-count` is zero, this does not spawn anything.
  (doseq [idx (range 0 (:shard-count test))]
    (mu/maybe-su test (mcontrol/exec test :mkdir :-p (mu/path-prefix test node (str "/data" idx)))))

  ;; Install the MongoDB binaries into the node's directory.
  (if-let [path (nth (re-find #"file://(.+)" url) 1)]
    (if (.isDirectory (io/file path))
      (install-local-directory! test node path)
      (install-local-tarball! test node path))
    (install-remote-tarball! test node url)))

(defn install!
  "Installs the MongoDB binaries into the node's directory from a local
  directory, a local tarball, or a remote tarball. Also creates a new user who
  owns the node's directory when running with some form of virtualization."
  [test node url]
  ; Add the "mongodb" user unless we are running without any virtualization.
  (when (= :vm (:virt test))
    (cu/ensure-user! (:username test)))

  (mu/maybe-su test (install-binaries! test node url))

  ; Set the permissions of the node's directory so that the "mongodb" user
  ; owns its contents unless we are running with out any virtualization.
  (when (= :vm (:virt test))
    (c/exec :chown :-R (str (:username test) ":" (:username test))
            (mu/path-prefix test node))))

(defn- log-files
  [test node]
  (cond-> [(mu/path-prefix test node "/stdout.log")
           (mu/path-prefix test node "/mongod.log")]
    (= :none (:virt test)) (conj (mu/path-prefix test node "/bridge.log"))))

(defn- strip-leading-slash
  "Removes the leading / from a pathname."
  [path]
  (str/replace path #"^/" ""))

(defn- remote-download!
  "Downloads the `remote` file to the `local` file location."
  [test node remote local]
  (info "downloading" remote "to" local)
  (try
    (->> local
         strip-leading-slash
         (store/path! test (name node))
         .getCanonicalPath
         (c/download remote))
    (catch java.io.IOException e
      (if (= "Pipe closed" (.getMessage e))
        (info remote "pipe closed")
        (throw e)))
    (catch java.lang.IllegalArgumentException e
      ; This is a JSch bug where the file is just being created.
      (info remote "doesn't exist"))))

(defn- local-copy!
  "Copies the `src` file to the `dest` file location."
  [test node src dest]
  (info "copying" src "to" dest)
  (try
    (->> dest
         strip-leading-slash
         (store/path! test (name node))
         (io/copy (io/file src)))
    (catch java.io.FileNotFoundException e
      (info src "doesn't exist"))))

;; Adapted from the jepsen.core/snarf-logs! function of Jepsen version 0.1.8
;; with support added to copy files locally to account for the possibility of
;; running without any virtualization.
(defn snarf-logs!
  "Downloads logs for a node."
  [test node]
  (let [full-paths (log-files test node)
        ; A map of full paths to short paths.
        paths      (->> full-paths
                        (map #(str/split % #"/"))
                        util/drop-common-proper-prefix
                        (map (partial str/join "/"))
                        (zipmap full-paths))]
    (doseq [[src dest] paths]
      (if (= :vm (:virt test))
        (remote-download! test node src dest)
        (local-copy! test node src dest)))))
