(ns tech.v3.datatype.copy-make-container
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy :as dtype-copy]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.casting :as casting]))


(defn copy!
  ([src dst options]
   (dtype-copy/copy! src dst (:unchecked? options)))
  ([src dst]
   (dtype-copy/copy! src dst true)))


(defmethod dtype-proto/make-container :jvm-heap
  [container-type datatype elem-seq-or-count options]
  (let [n-elems (long (if (number? elem-seq-or-count)
                        elem-seq-or-count
                        (dtype-base/ecount elem-seq-or-count)))
        ary-data
        (case (casting/host-flatten datatype)
          :boolean (boolean-array n-elems)
          :int8 (byte-array n-elems)
          :int16 (short-array n-elems)
          :char (char-array n-elems)
          :int32 (int-array n-elems)
          :int64 (long-array n-elems)
          :float32 (float-array n-elems)
          :float64 (double-array n-elems)
          (make-array (casting/datatype->object-class datatype) n-elems))
        ary-buf (array-buffer/array-buffer ary-data datatype)]
    (when-not (number? elem-seq-or-count)
      (copy! elem-seq-or-count ary-buf options))
    ary-buf))


(defmethod dtype-proto/make-container :java-array
  [container-type datatype elem-seq-or-count options]
  (dtype-proto/make-container :jvm-heap datatype elem-seq-or-count options))


;;Backwards compatibility
(defmethod dtype-proto/make-container :typed-buffer
  [container-type datatype elem-seq-or-count options]
  (dtype-proto/make-container :jvm-heap datatype elem-seq-or-count options))


(defmethod dtype-proto/make-container :native-heap
  [container-type datatype elem-seq-or-count options]
  (let [n-elems (long (if (number? elem-seq-or-count)
                        elem-seq-or-count
                        (dtype-base/ecount elem-seq-or-count)))
        n-bytes (* n-elems (casting/numeric-byte-width datatype))
        native-buf (-> (native-buffer/malloc n-bytes options)
                       (native-buffer/set-native-datatype datatype))]
    (when-not (number? elem-seq-or-count)
      (copy! elem-seq-or-count native-buf options))
    native-buf))


(defmethod dtype-proto/make-container :native-heap-LE
  [container-type datatype elem-seq-or-count options]
  (dtype-proto/make-container :native-heap datatype elem-seq-or-count
                              (assoc options :endianness :little-endian)))


(defmethod dtype-proto/make-container :native-heap-BE
  [container-type datatype elem-seq-or-count options]
  (dtype-proto/make-container :native-heap datatype elem-seq-or-count
                              (assoc options :endianness :big-endian)))


(defn make-container
  ([container-type datatype elem-seq-or-count options]
   (dtype-proto/make-container container-type datatype elem-seq-or-count options))
  ([container-type datatype elem-seq-or-count]
   (dtype-proto/make-container container-type datatype elem-seq-or-count {}))
  ([datatype elem-seq-or-count]
   (dtype-proto/make-container :jvm-heap datatype elem-seq-or-count {}))
  ([elem-seq-or-count]
   (when elem-seq-or-count
     (when (number? elem-seq-or-count)
       (throw (Exception. "Must provide existing container of items")))
     (dtype-proto/make-container :jvm-heap (dtype-base/elemwise-datatype elem-seq-or-count) elem-seq-or-count {}))))
