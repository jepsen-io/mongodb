(ns jepsen.mongodb.util
  "Utility functions."
  (:require [jepsen.control :as c]))

(defmacro maybe-sudo
  "Evaluates forms with a particular user unless `test` is running without any
  virtualization."
  [test user & body]
  `(if (= :vm (:virt ~test))
     (c/sudo ~user ~@body)
     ~@body))

(defmacro maybe-su
  "sudo root ... unless `test` is running without any virtualization."
  [test & body]
  `(if (= :vm (:virt ~test))
     (c/su ~@body)
     ~@body))

(defn path-prefix
  "Returns the subdirectory of the working directory that should be used for the
  node's data directory"
  [test node & suffixes]
  (apply str (:working-dir test) "/" (name node) suffixes))
