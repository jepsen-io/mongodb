(ns jepsen.mongodb.read-concern-majority-test
  "Read concern majority unit tests."
  (:require [clojure.test :refer :all]
            [jepsen.mongodb.read-concern-majority :as rcm]))

(deftest test-subset-chain
  (is (= true  (rcm/subset-chain?
                 [#{1 2 3} #{1 2 3 4} #{1 2 3 4 5 6} #{1 2 3 4 5 6 7}])))
  (is (= true  (rcm/subset-chain?
                 '(#{1 2 3} #{1 2 3 4} #{1 2 3 4 5 6} #{1 2 3 4 5 6 7}))))
  (is (= true  (rcm/subset-chain? [#{} #{} #{1}])))
  (is (= false (rcm/subset-chain? [#{} #{1} #{2}])))
  (is (= false (rcm/subset-chain? [#{1 2 3} #{1 2 3 4} #{1 2 3 5 7}])))
  (is (= false (rcm/subset-chain? [#{1 2} #{2 3} #{3 4}])))
  (is (= true  (rcm/subset-chain? [#{1 2} #{1 2 3}])))
  ; An empty sequence or a singleton sequence are both considered trivially
  ; valid subset chains.
  (is (= true (rcm/subset-chain? [])))
  (is (= true (rcm/subset-chain? [#{1 2}]))))
