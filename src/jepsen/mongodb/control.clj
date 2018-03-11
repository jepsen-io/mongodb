(ns jepsen.mongodb.control
  "Utility functions."
  (:require [jepsen.control :as c]
            [jepsen.control.local :as local]))

;;
;; exec methods
;;

(defmulti exec (fn [test & commands] (:virt test)))

(defmethod exec :none [test & commands]
  (apply local/exec commands))

(defmethod exec :vm [test & commands]
  (apply c/exec commands))
