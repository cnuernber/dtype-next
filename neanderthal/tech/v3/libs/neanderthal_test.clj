(ns tech.v3.libs.neanderthal-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype.argops :as dtype-ops]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.libs.neanderthal]
            [tech.v3.tensor :as dtt]
            [uncomplicate.neanderthal.core :as n-core]
            [uncomplicate.neanderthal.native :as n-native]))

(deftest basic-neanderthal-test
  (let [a (n-native/dge 3 3 (range 9))]
    (is (dfn/equals (dtt/ensure-tensor a)
                    (-> (dtt/->tensor (partition 3 (range 9)))
                        (dtt/transpose [1 0]))))
    (let [second-row (second (n-core/rows a))]
      (is (dfn/equals (dtt/ensure-tensor second-row)
                      [1 4 7])))))

(deftest basic-neanderthal-test-row-major
  (let [b (n-native/dge 3 3 (range 9) {:layout :row})]
    (is (dfn/equals (dtt/ensure-tensor b)
                    (dtt/->tensor (partition 3 (range 9)))))
    (let [second-row (second (n-core/rows b))]
      (is (dfn/equals (dtt/ensure-tensor second-row)
                      [3 4 5])))))

(deftest single-col-row-matrix
  (is (dfn/equals (dtt/ensure-tensor (n-native/dge 1 3 (range 3) {:layout :row}))
                  (dtt/->tensor (range 3))))

  (is (dfn/equals (dtt/ensure-tensor (n-native/dge 1 3 (range 3) {:layout :column}))
                  (dtt/->tensor (range 3))))

  (is (dfn/equals (dtt/ensure-tensor (n-native/dge 3 1 (range 3) {:layout :row}))
                  (dtt/->tensor (range 3))))

  (is (dfn/equals (dtt/ensure-tensor (n-native/dge 3 1 (range 3) {:layout :column}))
                  (dtt/->tensor (range 3)))))

(deftest argsort-supports-native
  (is (= [2 1 0] (dtype-ops/argsort (n-native/dv [3 2 1])))))
