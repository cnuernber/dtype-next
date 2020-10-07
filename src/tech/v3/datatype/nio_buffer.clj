(ns tech.v3.datatype.nio-buffer
  (:require [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.resource :as resource])
  (:import [java.nio Buffer ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer ByteOrder]
           [tech.v3.datatype UnsafeUtil]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [sun.misc Unsafe]))


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


(defn buffer-address
  ^long [^Buffer buf]
  (.getLong (native-buffer/unsafe) ^Object buf UnsafeUtil/addressFieldOffset))


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
                        (dtype-proto/sub-buffer offset# length#)))))
              dtype-proto/PToNativeBuffer
              (convertible-to-native-buffer? [buf#]
                (.isDirect (datatype->nio-buf ~dtype buf#)))
              (->native-buffer [buf#]
                (native-buffer/wrap-address
                 (buffer-address buf#)
                 (* (dtype-proto/ecount buf#)
                    (casting/numeric-byte-width (dtype-proto/elemwise-datatype buf#)))
                 (dtype-proto/elemwise-datatype buf#)
                 (dtype-proto/endianness buf#)
                 buf#))))))))


(extend-nio-types)


(defn native-buf->nio-buf
  ^java.nio.Buffer [^NativeBuffer buffer]
  (let [dtype (dtype-proto/elemwise-datatype buffer)
        byte-width (casting/numeric-byte-width dtype)
        n-bytes (* (.n-elems buffer) byte-width)
        ^ByteBuffer byte-buf (UnsafeUtil/constructByteBufferFromAddress
                              (.address buffer) n-bytes)]
    (.order byte-buf
            (case (.endianness buffer)
              :little-endian ByteOrder/LITTLE_ENDIAN
              :big-endian ByteOrder/BIG_ENDIAN))
    (resource/track
     (case (casting/host-flatten dtype)
       :int8 byte-buf
       :int16 (.asShortBuffer byte-buf)
       :int32 (.asIntBuffer byte-buf)
       :int64 (.asLongBuffer byte-buf)
       :float32 (.asFloatBuffer byte-buf)
       :float64 (.asDoubleBuffer byte-buf))
     ;;Link the two of them via the GC.
     #(constantly buffer) :gc)))
