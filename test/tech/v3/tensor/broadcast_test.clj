(ns tech.v3.tensor.broadcast-test
  (:require [tech.v3.tensor :as dtt]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer :all]))


(deftest simple-broadcast
  (let [bcast-tens (dtt/broadcast (dtt/->tensor [5 6 7])
                                  [3 3])]
    (is (dfn/equals [5 6 7 5 6 7 5 6 7]
                    (dtype/->reader bcast-tens)))
    (is (dfn/equals (dtt/->tensor [[5 6 7] [5 6 7] [5 6 7]])
                    bcast-tens))))
