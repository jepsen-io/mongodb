(ns jepsen.control.local-test
  "Tests for the jepsen.control.local namespace."
  (:require [clojure.test :refer :all]
            [jepsen.control :as c]
            [jepsen.control.local :as clocal]))

(deftest echo-message
  (is (= "hello there" (clocal/exec :echo "hello there"))))

(deftest change-directory
  (is (= "/tmp" (c/cd "/tmp" (clocal/exec :pwd)))))
