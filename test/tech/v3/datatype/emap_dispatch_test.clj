(ns tech.v3.datatype.emap-dispatch-test
  (:require [tech.v3.datatype.emap :refer [emap]]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.op-dispatch :refer [dispatch-binary-op dispatch-unary-op]]
            [tech.v3.datatype.array-buffer :as abuf]
            [tech.v3.datatype.protocols :refer [elemwise-datatype] :as dt-proto]
            [tech.v3.datatype.primitive]
            [ham-fisted.api :as hamf]
            [clojure.test :refer [deftest is]]))

(deftest unary-emap-types
  (let [odata [1 2 3 4]
        emap-odata (fn
                     ([a] (emap a nil odata))
                     ([a odata] (emap a nil odata)))
        emap-dt (fn
                  ([a] (elemwise-datatype (emap-odata a)))
                  ([a odata] (elemwise-datatype (emap-odata a odata))))
        ol (fn ^long [a] (long a))
        ll (fn ^long [^long a] a)
        od (fn ^double [a] (double a))
        dd (fn ^double [^double a] a)
        oo (fn [a] a)
        nested-dt (fn [a b odata] (->> (emap-odata a odata)
                                       (emap-odata b)
                                       (elemwise-datatype)))]
    (is (= :int64 (emap-dt ol)))
    (is (= :float64 (emap-dt od)))
    (is (= :object (emap-dt oo)))
    (is (= :int64 (emap-dt ol)))
    (is (= :float64 (emap-dt od)))
    (is (= :int64 (nested-dt od ol odata)))
    (is (= :float64 (nested-dt od dd odata)))
    (is (= :object (nested-dt oo oo odata)))
    (is (= :int64 (nested-dt ol ll odata)))
    (is (= :float64 (nested-dt od dd odata)))))


(deftest ol-pathway-test
  (let [ra-in (emap unary-op/log1p nil (vec (range 10 100 10)))
        seq-in (emap unary-op/log1p nil (seq (vec (range 10 100 10))))
        ra-res (emap unary-op/round nil ra-in)
        seq-res (emap unary-op/round nil seq-in)]
    (is (instance? java.util.RandomAccess ra-res))
    (is (not (instance? java.util.RandomAccess seq-res)))
    (is (= [2 3 3 4 4 4 4 4 5] ra-res))
    (is (= [2 3 3 4 4 4 4 4 5] seq-res))
    (is (= :float64 (elemwise-datatype ra-in)))
    (is (= :float64 (elemwise-datatype seq-in)))

    (is (= 33 (reduce + 0 ra-res)))
    (is (= 33 (reduce + 0 seq-res)))))

(deftype LongDouble [data]
  dt-proto/PElemwiseDatatype (elemwise-datatype [this] :int64)
  dt-proto/POperationalElemwiseDatatype (operational-elemwise-datatype [this] :float64)
  dt-proto/PElemwiseReaderCast (elemwise-reader-cast [this dt]
                                 (case dt
                                   :int64 (dt-proto/->reader (long-array data))
                                   :float64 (dt-proto/->reader (double-array data))))
  dt-proto/PECount (ecount [this] (count data))
  dt-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this] (dt-proto/elemwise-reader-cast [this :int64])))


(deftest bin-op-dtype-test
  (is (= :float64 (elemwise-datatype (emap binary-op/+ nil (LongDouble. (range 10)) 5))))
  (is (= :float64 (elemwise-datatype (emap binary-op/+ :float64 (LongDouble. (range 10)) 5))))
  (is (= :float64 (elemwise-datatype (emap binary-op/+ :float64 (LongDouble. (range 10)) (long-array (repeat 10 5))))))
  (is (= 95.0 (hamf/sum (emap binary-op/+ nil (LongDouble. (range 10)) 5))))
  (is (= 95.0 (hamf/sum (emap binary-op/+ :float64 (LongDouble. (range 10)) 5))))
  (is (= 95.0 (hamf/sum (emap binary-op/+ :float64 (LongDouble. (range 10)) (long-array (repeat 10 5))))))
  
  )
