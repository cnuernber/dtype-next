(ns tech.v3.datatype.array-buffer
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.casting :as casting]
            [primitive-math :as pmath]))


(def array-types
  (vec (concat casting/host-numeric-types
               [:boolean :character :object])))


(defmacro initial-implement-arrays
  []
  `(do ~@(->> array-types
              (map (fn [ary-type]
                     `(extend-type ~(typecast/datatype->array-cls ary-type)
                        dtype-proto/PElemwiseDatatype
                        (elemwise-datatype [item#] ~ary-type)))))))


(initial-implement-arrays)


(defmacro java-array->reader->reader
  [datatype cast-dtype advertised-datatype buffer java-ary offset n-elems]
  `(let [~'java-ary (typecast/datatype->array ~datatype ~java-ary)]
     (reify
       dtype-proto/PToArrayBuffer
       (convertible-to-array-buffer? [this#] true)
       (->array-buffer [this#] ~buffer)
       ;;Forward protocol methods that are efficiently implemented by the buffer
       dtype-proto/PBuffer
       (sub-buffer [this# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->reader {})))

       ~(typecast/datatype->reader-type (casting/safe-flatten datatype))
       (getElemwiseDatatype [rdr#] ~advertised-datatype)
       (lsize [rdr#] ~n-elems)
       (read [rdr# ~'idx]
         ~(case datatype
            ;; :int8 `(.getByte (unsafe) (pmath/+ ~address ~'idx))
            ;; :uint8 `(-> (.getByte (unsafe) (pmath/+ ~address ~'idx))
            ;;             (pmath/byte->ubyte))
            ;; :int16 `(.getShort (unsafe) (pmath/+ ~address
            ;;                                      (pmath/* ~'idx ~byte-width)))
            ;; :uint16 `(-> (.getShort (unsafe) (pmath/+ ~address
            ;;                                           (pmath/* ~'idx ~byte-width)))
            ;;              (pmath/short->ushort))
            ;; :int32 `(.getInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
            ;; :uint32 `(-> (.getInt (unsafe) (pmath/+ ~address
            ;;                                         (pmath/* ~'idx ~byte-width)))
            ;;              (pmath/int->uint))
            ;; :int64 `(.getLong (unsafe) (pmath/+ ~address
            ;;                                     (pmath/* ~'idx ~byte-width)))
            ;; :uint64 `(-> (.getLong (unsafe) (pmath/+ ~address
            ;;                                          (pmath/* ~'idx ~byte-width))))
            ;; :float32 `(.getFloat (unsafe) (pmath/+ ~address
            ;;                                        (pmath/* ~'idx ~byte-width)))
            :float64 `(casting/datatype->unchecked-cast-fn
                       ~datatype ~cast-dtype
                       (aget ~'java-ary (pmath/+ ~offset ~'idx))))))))



(deftype ArrayBuffer [ary-data ^int offset ^int n-elems datatype]
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [item] datatype)
  dtype-proto/PECount
  (ecount [item] n-elems)
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item] true)
  (->array-buffer [item] item)
  dtype-proto/PBuffer
  (sub-buffer [item off len]
    (ArrayBuffer. ary-data
                  (+ offset (int off))
                  (int len)
                  datatype))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (case [(dtype-proto/elemwise-datatype ary-data)
           (casting/un-alias-datatype datatype)]
      [:float64 :float64] (java-array->reader->reader :float64 :float64 datatype item
                                                      ary-data offset n-elems))))


(defn array-buffer
  [java-ary]
  (ArrayBuffer. java-ary 0 (count java-ary) (dtype-proto/elemwise-datatype java-ary)))
