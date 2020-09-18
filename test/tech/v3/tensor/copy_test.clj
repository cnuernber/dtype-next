(ns tech.v3.tensor.copy-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.tensor :as dtt]
            [clojure.test :refer :all]))


(deftest tensor-copy-test
  (let [new-tens (repeat 2 (dtt/->tensor [[2] [3]]))
        dest-tens (dtt/new-tensor [2 2])]
    (dtype/copy-raw->item! (->> new-tens
                                (map #(dtype/select % 0 :all)))
                           (dtype/select dest-tens 0 :all))
    (is (dfn/equals dest-tens (dtt/->tensor [[2 2] [0 0]])))
    (dtype/copy-raw->item! (->> new-tens
                                (map #(dtype/select % 1 :all)))
                           (dtype/select dest-tens 1 :all))
    (is (dfn/equals dest-tens (dtt/->tensor [[2 2] [3 3]])))))
