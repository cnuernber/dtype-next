(ns tech.v3.datatype.cheatsheet-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.tensor :as tens]
            [tech.v3.datatype.functional :as dtype-fn]
            [tech.v3.datatype.argops :as argops]
            [clojure.test :refer :all]))



(deftest cheatsheet-test
  (let [float-data (dtype/copy! (range 10) (float-array 10))]
    (is (= [0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0]
           (dtype/->vector float-data))))
  (let [test-data (dtype-fn/+ (range 10 0 -1) 5)
        indexes (argops/argsort test-data)]
    (is (= [6 7 8 9 10 11 12 13 14 15]
           (dtype/indexed-buffer indexes (vec test-data)))))

  (let [test-tens (tens/->tensor (partition 3 (range 9)))
        ;;This actually tests quite a lot but type promotion is one
        ;;thing.
        result-tens (dtype-fn/+ 2 (tens/select test-tens [1 0]))]
    (is (= [[5 6 7]
            [2 3 4]]
           (tens/->jvm result-tens)))))
