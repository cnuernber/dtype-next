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


(deftest ptr-size-t-types
  (let [sdef (dt-struct/define-datatype! :ptr-types [{:name :a :datatype :pointer}
                                                     {:name :b :datatype :size-t}
                                                     {:name :c :datatype :offset-t}
                                                     {:name :d :datatype :int64}])
        data (dt-struct/new-struct :ptr-types)
        sarray (dt-struct/new-array-of-structs :ptr-types 10)]
    (is (= (vec (repeat 10 0))
           (vec (dt-struct/array-of-structs->column sarray :a))))))


(deftest get-set-array-members
  (let [sdef (dt-struct/define-datatype! :array-member [{:name :x :datatype :int32 :n-elems 10}])
        vec-type (dt-struct/define-datatype! :vec3 [{:name :x :datatype :float32}
                                                    {:name :y :datatype :float32}
                                                    {:name :z :datatype :float32}])
        ttype (dt-struct/define-datatype! :triangle [{:name :pts :datatype :vec3 :n-elems 3}])
        sdata (dt-struct/new-struct :array-member)
        tdata (dt-struct/new-struct :triangle)
        x (get sdata :x)
        _ (do 
            ;;sdata'x X member is a reified LongBuffer so you can set the value via getting it then dtype/copy!
            (dtype/copy! (range 10) x)
            (is (= (vec (range 10)) (get sdata :x))))
        ys (dt-struct/array-of-structs->column (:pts tdata) :y)
        _ (do
            (dtype/copy! (range 1 4) ys)
            (is (= 2.0 (get-in tdata [:pts 1 :y]))))]))
