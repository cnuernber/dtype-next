(ns tech.v3.datatype.nippy-test
  (:require [tech.v3.datatype.functional :as dfn]
            [tech.v3.tensor :as dtt]
            [tech.v3.datatype.nippy]
            [taoensso.nippy :as nippy]
            [clojure.test :refer [deftest is]]))


(deftest various-nippy
  (let [jvm-tens (dtt/->tensor (partition 3 (range 9))
                               :datatype :float64)
        native-tens (dtt/->tensor (partition 3 (range 9))
                                  :datatype :float64
                                  :container-type :native-heap)
        nippy-data (nippy/freeze [jvm-tens native-tens])
        [test-jvm-tens test-native-tens] (nippy/thaw nippy-data)]
    (is (dfn/equals jvm-tens native-tens))
    (is (dfn/equals jvm-tens test-jvm-tens))
    (is (dfn/equals jvm-tens test-native-tens))))
