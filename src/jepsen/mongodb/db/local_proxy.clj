(ns jepsen.mongodb.db.local-proxy
  "Sets up a local binary which proxies to remote MongoDB nodes."
  (:require [byte-streams :as bs]
            [clojure [pprint :refer [pprint]]
                     [string :as str]]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [real-pmap]]
            [jepsen [control :as c]
                    [core :as jepsen]
                    [db :as db]
                    [util :as util :refer [meh
                                           pprint-str
                                           random-nonempty-subset
                                           with-thread-name]]]
            [jepsen.control [net :as cn]
                            [util :as cu]]
            [jepsen.lazyfs :as lazyfs]
            [jepsen.os.debian :as debian]
            [jepsen.mongodb [client :as client :refer [Conn
                                                       host
                                                       port]]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.lang Process
                      ProcessBuilder
                      ProcessBuilder$Redirect)
           (java.io File
                    IOException
                    OutputStreamWriter
                    Writer)
           (java.util.concurrent TimeUnit)))

(def proxy-listener-count
  "How many proxy listener ports do we bind?"
  3)

(defmacro io-thread
  "Stolen from Maelstrom.

  Spawns an IO thread for a process. Takes a running? atom, a thread name (e.g.
  \"stdin\"), [sym closable-expression ...] bindings (for with-open), a single
  loop-recur binding, and a body. Spawns a future, holding the closeable open,
  evaluating body in the loop-recur bindings as long as `running?` is true, and
  catching/logging exceptions. Body should return the next value for the loop
  iteration, or `nil` to terminate."
  [running? thread-type open-bindings loop-binding & body]
  `(future
     (with-thread-name (str "proxy " ~thread-type)
       (try
         (with-open ~open-bindings
           ; There is technically a race condition here: we might be
           ; interrupted during evaluation of the loop bindings, *before* we
           ; enter the try. Hopefully infrequent. If it happens, not the end of
           ; the world; just yields a confusing error message, maybe some weird
           ; premature closed-stream behavior.
           (loop ~loop-binding
             (if-not (deref ~running?)
               ; We're done
               :done
               (recur (try ~@body
                           (catch IOException e#
                             ; If the process crashes, we're going to hit
                             ; IOExceptions trying to write/read streams.
                             ; That's fine--we're going to learn about crashes
                             ; when the process shutdown code checks the exit
                             ; status.
                             )
                           (catch InterruptedException e#
                             ; We might be interrupted if setup fails, but it's
                             ; not our job to exit here--we need to keep the
                             ; process's streams up and running so we can tell
                             ; if it terminated normally. We'll be terminated
                             ; by the DB teardown process.
                             )
                           (catch Throwable t#
                             (warn t# "Error!")
                             nil))))))
         (catch IOException e#
           ; with-open is going to try to close things like OutputWriters,
           ; which will actually throw if the process has crashed, because they
           ; try to flush the underlying stream buffer, and THAT's closed. We
           ; ignore that too; the process shutdown code will alert the user.
           :crashed
           )))))

(defn journal-thread
  "Starts a thread which copies :stdout or :stderr to a log: an atom of string
  lines."
  [^Process process running? type log]
  (io-thread running? (name type)
             []
             [lines (bs/to-line-seq (case type
                                      :stderr (.getErrorStream process)
                                      :stdout (.getInputStream process)))]
             (when (seq lines)
               (let [line (first lines)]
                 (info "Logging" type line)
                 (swap! log conj line))
               (next lines))))

(defn start!
  "Starts the proxy locally for a test."
  [test db node]
  (let [proxy (:local-proxy test)
        bin   (if (re-find #"/" proxy)
                proxy
                (str "./" proxy))
        args ["-mongo-upstream" (str (cn/ip node) ":" (port db test))
              "-listener-count" proxy-listener-count]
        _ (info "Launching" bin args)
        process (.. (ProcessBuilder. ^java.util.List (map str (cons bin args)))
                    (redirectOutput ProcessBuilder$Redirect/PIPE)
                    (redirectInput  ProcessBuilder$Redirect/PIPE)
                    (start))
        running? (atom true)
        stdout   (atom [])
        stderr   (atom [])
        stdout-thread (journal-thread process running? :stdout stdout)
        stderr-thread (journal-thread process running? :stderr stderr)]
    {:process process
     :running? running?
     :stdout   stdout
     :stderr   stderr
     :stdout-thread stdout-thread
     :stderr-thread stderr-thread}))

(defn stop!
  "Stops the local proxy. Takes the same map returned by start!"
  [{:keys [^Process process running? stdout stderr stdout-thread
           stderr-thread]}]
  (let [crashed? (not (.isAlive process))]
    (when-not crashed?
      ; Kill
      (.. process destroyForcibly (waitFor 5 (TimeUnit/SECONDS))))

    ; Shut down workers
    (reset! running? false)
    @stdout-thread
    @stderr-thread

    (when crashed?
      (throw+ {:type ::crashed
               :exit (.exitValue process)}
              nil
              (str "Local proxy crashed with exit status "
                   (.exitValue process)
                   ". Before crashing, it wrote to STDOUT:\n\n"
                   @stdout
                   "\n\nAnd to STDERR:\n\n"
                   @stderr)))))

(defrecord LocalProxyDB [db proxy]
  Conn
  (host [_ test node]
    "localhost")

  (port [_ test]
    (+ client/proxy-port (rand-int proxy-listener-count)))

  db/DB
  (setup! [this test node]
    (db/setup! db test node)
    (when (and (:local-proxy test)
               (= node (jepsen/primary test)))
      (deliver proxy (start! test db node))
      (with-open [conn (client/await-open node test)]
        ; Huh, it doesn't SUPPORT rs_status. Weird.
        ;(info "Proxy reports rs_status"
        ;      (pprint-str
        ;        (client/admin-command! conn {:replSetGetStatus 1})
                )))

  (teardown! [this test node]
    (db/teardown! db test node)
    (when (and (realized? proxy)
               (= node (jepsen/primary test)))
      (stop! @proxy)
      (with-open [w (io/writer "store/current/local-proxy.txt")]
        (binding [*out* w]
          (doseq [line (concat @(:stdout @proxy)
                               @(:stderr @proxy))]
            (println line))))))

  db/Primary
  (setup-primary! [_ test node]
    (db/setup-primary! db test node))

  (primaries [_ test]
    (db/primaries db test))

  db/LogFiles
  (log-files [_ test node]
    (db/log-files db test node))

  ; We don't bother injecting faults into the proxy itself
  db/Process
  (start! [_ test node]
    (db/start! db test node))

  (kill! [_ test node]
    (db/kill! db test node))

  db/Pause
  (pause! [_ test node]
    (db/pause! db test node))

  (resume! [this test node]
    (db/resume! db test node)))

(defn db
  "Constructs a LocalProxy DB wrapping another DB."
  [db]
  (LocalProxyDB. db (promise)))
