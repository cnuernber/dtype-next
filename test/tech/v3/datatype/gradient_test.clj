(ns tech.v3.datatype.gradient-test
  (:require [clojure.test :refer [deftest testing is]]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.gradient :as dt-grad]))


(deftest gradient1d-examples
  (testing "gradient1d"
    (testing "conforms to the NumPy examples"
      (is
        (dfn/equals
          [1.0 1.5 2.5 3.5 4.5 5.0]
          (dt-grad/gradient1d [1 2 4 7 11 16])))
      (is
        (dfn/equals
          [0.5 0.75 1.25 1.75 2.25 2.5]
          (dt-grad/gradient1d [1 2 4 7 11 16] 2.))))
    (testing "uses dx=1 by default"
      (is
        (dfn/equals
          (dt-grad/gradient1d [1 2 4 7 11 16])
          (dt-grad/gradient1d [1 2 4 7 11 16] 1.))))
    (testing "may use negative dx"
      (is
        (dfn/equals
          [-1.0 -1.5 -2.5 -3.5 -4.5 -5.0]
          (dt-grad/gradient1d [1 2 4 7 11 16] -1.))))
    (testing "yields constant df/dx for linear data"
      (dfn/equals
        (repeat 10 1.)
        (dt-grad/gradient1d (range 10))))
    (testing "decreases as dx increases"
      (is
        (dfn/equals
          [1.0 1.5 2.5 3.5 4.5 5.0]
          (dt-grad/gradient1d [1 2 4 7 11 16] 1.)))
      (is
        (dfn/equals
          [0.1 0.15 0.25 0.35 0.45 0.5]
          (dt-grad/gradient1d [1 2 4 7 11 16] 10.)))
      (is
        (dfn/equals
          (dt-grad/gradient1d [1 2 4 7 11 16] 1.)
          (-> (dt-grad/gradient1d [1 2 4 7 11 16] 10.) (dfn/* 10.)))))))
