(ns tech.v3.datatype.struct-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.struct :as dt-struct]
            [clojure.test :refer [deftest is]]))


(deftest base-simple-struct
  (let [vec-type (dt-struct/define-datatype! :vec3 [{:name :x :datatype :float32}
                                                    {:name :y :datatype :float32}
                                                    {:name :z :datatype :float32}])
        test-data (dt-struct/new-struct :vec3)]
    (is (dfn/equals [0.0 0.0 0.0]
                    (mapv test-data [:x :y :z])))
    (.put test-data :x 3.0)
    (is (dfn/equals [3.0 0.0 0.0]
                    (mapv test-data [:x :y :z])))
    (is (dfn/equals [3.0]
                    [(:x test-data)])))
  (let [test-data (dt-struct/new-struct :vec3 {:container-type :native-heap})]
    (is (not (nil? (dtype/->native-buffer test-data))))))
