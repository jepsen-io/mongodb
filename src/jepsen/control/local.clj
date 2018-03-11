(ns jepsen.control.local
  "Functions for running commands against the local host.

  This namespace presents an API similar to that of the `jepsen.control`
  namespace with the behavioral difference being these functions run commands
  against the local host (via bash) rather than the remote host (via bash over
  the node's SSH connection)."
  (:require [clojure.string :as str]
            [clojure.java.shell :as shell]
            [jepsen.control :as c]))

(defn wrap-sudo
  "Wraps command in a sudo subshell."
  [cmd]
  (if c/*sudo*
    (merge cmd {:cmd `("sudo" "-S" "-u" ~c/*sudo* "--" ~@(:cmd cmd))
                :in  (if c/*password*
                       (str c/*password* "\n" (:in cmd))
                       (:in cmd))})
    cmd))

(defn wrap-bash
  "Wraps command in a bash shell."
  [cmd]
  (assoc cmd :cmd `("bash" "-c" ~(str/join " " (:cmd cmd)))))

(defn wrap-cd
  "Wraps command by changing to the current bound directory first."
  [cmd]
  (if c/*dir*
    (assoc cmd :dir c/*dir*)
    cmd))

(defn sh*
  "Evaluates a shell command."
  [{cmd :cmd, :as action}]
  (apply shell/sh (concat cmd (mapcat identity (dissoc action :cmd)))))

(defn exec*
  "Like exec, but does not escape."
  [& commands]
  (-> (array-map :cmd commands)
      wrap-cd
      wrap-bash
      wrap-sudo
      c/wrap-trace
      sh*
      c/throw-on-nonzero-exit
      c/just-stdout))

(defn exec
  "Takes a shell command and arguments, runs the command on the local host,
  and returns its stdout, throwing if an error occurs. Escapes all arguments."
  [& commands]
  (->> commands
       (map c/escape)
       (apply exec*)))
