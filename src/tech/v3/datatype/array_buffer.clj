(ns tech.v3.datatype.array-buffer
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.pprint :as dtype-pp]
            [primitive-math :as pmath])
  (:import [clojure.lang IObj Counted Indexed IFn]
           [tech.v3.datatype PrimitiveIO]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro java-array-buffer->io
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

       ~(typecast/datatype->io-type (casting/safe-flatten cast-dtype))
       (elemwiseDatatype [rdr#] ~advertised-datatype)
       (lsize [rdr#] ~n-elems)
       (read [rdr# ~'idx]
         (casting/datatype->unchecked-cast-fn
          ~datatype ~cast-dtype
          (aget ~'java-ary (pmath/+ ~offset ~'idx))))
       (write [wtr# idx# val#]
         ;;Writing values is always checked, no options.
         (aset ~'java-ary (pmath/+ ~offset idx#)
               (casting/datatype->cast-fn
                ~cast-dtype ~datatype
                val#))))))


(declare array-buffer->io)


(deftype ArrayBuffer [ary-data ^int offset ^int n-elems datatype metadata
                      ^:volatile-mutable ^PrimitiveIO cached-io]
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [item] datatype)
  dtype-proto/PECount
  (ecount [item] n-elems)
  dtype-proto/PEndianness
  (endianness [item#] :big-endian)
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item] true)
  (->array-buffer [item] item)
  dtype-proto/PBuffer
  (sub-buffer [item off len]
    (ArrayBuffer. ary-data
                  (+ offset (int off))
                  (int len)
                  datatype
                  metadata
                  nil))
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [item] true)
  (->primitive-io [item]
    (if cached-io cached-io
        (let [io
              (array-buffer->io ary-data datatype item offset n-elems)]
          (set! cached-io io)
          io)))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (dtype-proto/->primitive-io item))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (dtype-proto/->primitive-io item))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (ArrayBuffer. ary-data offset n-elems datatype metadata cached-io))
  Counted
  (count [item] (int (dtype-proto/ecount item)))
  Indexed
  (nth [item idx]
    ((dtype-proto/->primitive-io item) idx))
  (nth [item idx def-val]
    (if (and (>= idx 0) (< idx (.count item)))
      ((dtype-proto/->primitive-io item) idx)
      def-val))
  IFn
  (invoke [item idx]
    (nth item (int idx)))
  (invoke [item idx value]
    ((dtype-proto/->writer item {}) idx value))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  Object
  (toString [item]
    (dtype-pp/buffer->string item "array-buffer")))


(dtype-pp/implement-tostring-print ArrayBuffer)


(defn- array-buffer->io
  [ary-data datatype ^ArrayBuffer item offset n-elems]
  (let [offset (int offset)
        n-elems (int n-elems)]
    (case [(dtype-proto/elemwise-datatype ary-data)
           (casting/un-alias-datatype datatype)]
      [:boolean :boolean] (java-array-buffer->io :boolean :boolean datatype item
                                                 ary-data offset n-elems)
      [:int8 :uint8] (java-array-buffer->io :int8 :uint8 datatype item
                                            ary-data offset n-elems)
      [:int8 :int8] (java-array-buffer->io :int8 :int8 datatype item
                                           ary-data offset n-elems)
      [:int16 :uint16] (java-array-buffer->io :int16 :uint16 datatype item
                                              ary-data offset n-elems)
      [:int16 :int16] (java-array-buffer->io :int16 :int16 datatype item
                                             ary-data offset n-elems)
      [:char :char] (java-array-buffer->io :char :char datatype item
                                           ary-data offset n-elems)
      [:int32 :uint32] (java-array-buffer->io :int32 :uint32 datatype item
                                              ary-data offset n-elems)
      [:int32 :int32] (java-array-buffer->io :int32 :int32 datatype item
                                             ary-data offset n-elems)
      [:int64 :uint64] (java-array-buffer->io :int64 :uint64 datatype item
                                              ary-data offset n-elems)
      [:int64 :int64] (java-array-buffer->io :int64 :int64 datatype item
                                             ary-data offset n-elems)
      [:float32 :float32] (java-array-buffer->io :float32 :float32 datatype item
                                                 ary-data offset n-elems)
      [:float64 :float64] (java-array-buffer->io :float64 :float64 datatype item
                                                 ary-data offset n-elems)
      (java-array-buffer->io :object :object datatype item ary-data offset n-elems))))


(defn array-buffer
  ([java-ary]
   (ArrayBuffer. java-ary 0 (count java-ary)
                 (dtype-proto/elemwise-datatype java-ary)
                 {}
                 nil))
  ([java-ary buf-dtype]
   (ArrayBuffer. java-ary 0 (count java-ary) buf-dtype {}
                 nil)))


(defn array-buffer->map
  "Convert an array buffer to a map of
  {:java-array :offset :length :datatype}"
  [^ArrayBuffer ary-buf]
  {:java-array (.ary-data ary-buf)
   :offset (.offset ary-buf)
   :length (.n-elems ary-buf)
   :datatype (.datatype ary-buf)
   :metadata (.metadata ary-buf)})


(defn is-array-type?
  [item]
  (when item
    (.isArray (.getClass ^Object item))))


(def array-types
  (set (concat casting/host-numeric-types
               [:boolean :object :char])))


(defn- as-object ^Object [item] item)


(defmacro initial-implement-arrays
  []
  `(do ~@(->> array-types
              (map (fn [ary-type]
                     `(extend-type ~(typecast/datatype->array-cls ary-type)
                        dtype-proto/PElemwiseDatatype
                        (elemwise-datatype [~'item]
                          ~(if (not= ary-type :object)
                             `~ary-type
                             `(-> (as-object ~'item)
                                  (.getClass)
                                  (.getComponentType)
                                  (casting/object-class->datatype))))
                        dtype-proto/PECount
                        (ecount [item#]
                          (alength
                           (typecast/datatype->array ~ary-type item#)))
                        ;;Java stores array data in memory as big-endian.  Holdover
                        ;;from Sun Spark architecture.
                        dtype-proto/PEndianness
                        (endianness [item#] :big-endian)
                        dtype-proto/PToArrayBuffer
                        (convertible-to-array-buffer? [item#] true)
                        (->array-buffer [item#]
                          (ArrayBuffer. item# 0
                                        (alength (typecast/datatype->array ~ary-type
                                                                           item#))
                                        (dtype-proto/elemwise-datatype item#)
                                        {}
                                        nil))
                        dtype-proto/PBuffer
                        (sub-buffer [item# off# len#]
                          (ArrayBuffer. item#
                                        (int off#)
                                        (int len#)
                                        (dtype-proto/elemwise-datatype item#)
                                        {}
                                        nil))
                        dtype-proto/PToReader
                        (convertible-to-reader? [item#] true)
                        (->reader [item# options#]
                          (dtype-proto/->reader
                           (array-buffer item#)
                           options#))
                        dtype-proto/PToWriter
                        (convertible-to-writer? [item#] true)
                        (->writer [item# options#]
                          (dtype-proto/->writer
                           (array-buffer item#)
                           options#))))))))


(initial-implement-arrays)
