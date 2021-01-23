(ns tech.v3.datatype.nio-buffer-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [clojure.test :refer [deftest is]]))


(deftest simple-nio-conversion
  (let [data (dtype/make-container :native-heap :uint8 [255 254 0 1])
        bdata (nio-buffer/->nio-buffer data)
        nbuf (-> (dtype/as-native-buffer bdata)
                 (native-buffer/set-native-datatype :uint8))]
    (is (= (vec (dtype/->short-array data))
           (vec (dtype/->short-array nbuf))))))
