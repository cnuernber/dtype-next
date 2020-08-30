(ns tech.v3.datatype.nio-buffer
  (:require [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting])
  (:import [java.nio Buffer ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer]))


(defn datatype->nio-buf-type
  [datatype]
  (case (casting/host-flatten datatype)
    :int8 'java.nio.ByteBuffer
    :int16 'java.nio.ShortBuffer
    :int32 'java.nio.IntBuffer
    :int64 'java.nio.LongBuffer
    :float32 'java.nio.FloatBuffer
    :float64 'java.nio.DoubleBuffer))


(defn as-byte-buffer ^ByteBuffer [item] item)
(defn as-short-buffer ^ShortBuffer [item] item)
(defn as-int-buffer ^IntBuffer [item] item)
(defn as-long-buffer ^LongBuffer [item] item)
(defn as-float-buffer ^FloatBuffer [item] item)
(defn as-double-buffer ^DoubleBuffer [item] item)



(defmacro datatype->nio-buf
  [datatype item]
  (case (casting/host-flatten datatype)
    :int8 `(as-byte-buffer ~item)
    :int16 `(as-short-buffer ~item)
    :int32 `(as-int-buffer ~item)
    :int64 `(as-long-buffer ~item)
    :float32 `(as-float-buffer ~item)
    :float64 `(as-double-buffer ~item)))


(def nio-datatypes #{:int8 :int16 :int32 :int64 :float32 :float64})


(def buffer-address
  (let [addr-field (.getDeclaredField Buffer "address")
        offset (.objectFieldOffset (native-buffer/unsafe) addr-field)]
    (fn [^Buffer buf]
      (.getLong (native-buffer/unsafe) ^Object buf (long offset)))))


(defmacro extend-nio-types
  []
  `(do
     ~@(->>
        nio-datatypes
        (map
         (fn [dtype]
           `(extend-type ~(datatype->nio-buf-type dtype)
              dtype-proto/PElemwiseDatatype
              (elemwise-datatype [buf#] ~dtype)
              dtype-proto/PECount
              (ecount [buf#] (.remaining (datatype->nio-buf ~dtype buf#)))
              dtype-proto/PToArrayBuffer
              (convertible-to-array-buffer? [buf#]
                (not (.isDirect (datatype->nio-buf ~dtype buf#))))
              (->array-buffer [buf#]
                (let [buf# (datatype->nio-buf ~dtype buf#)
                      offset# (.position buf#)
                      length# (.remaining buf#)]
                  (when-not (.isDirect buf#)
                    (-> (array-buffer/array-buffer (.array buf#))
                        (dtype-proto/sub-buffer offset# length#)))))))))))


(extend-nio-types)
