(ns tech.v3.libs.javacpp
  (:require [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.native-buffer :as nbuf]
            [tech.v3.datatype.casting :as casting]
            [ham-fisted.defprotocol :refer [extend]])
  (:import [org.bytedeco.javacpp DoublePointer FloatPointer IntPointer LongPointer
            ShortPointer BytePointer Pointer]
           [tech.v3.datatype.native_buffer NativeBuffer])
  (:refer-clojure :exclude [extend]))


(defn javacpp-pointer->native-buffer
  ^NativeBuffer [^Pointer ptr datatype]
  (let [addr (.address ptr)
        n-elems (- (.limit ptr) (.position ptr))
        bytes-ptr-elem (casting/numeric-byte-width datatype)]
    (nbuf/wrap-address addr (* n-elems bytes-ptr-elem) datatype (dt-proto/platform-endianness) ptr)))


(doseq [[dtype ptr-type] {:int8 BytePointer
                          :int16 ShortPointer
                          :int32 IntPointer
                          :int64 LongPointer
                          :float32 FloatPointer
                          :float64 DoublePointer}]
  (extend ptr-type
    dt-proto/PElemwiseDatatype
    {:elemwise-datatype (constantly dtype)}
    dt-proto/PToNativeBuffer
    {:convertible-to-native-buffer?  (constantly true)
     :->native-buffer
     #(javacpp-pointer->native-buffer % dtype)}))
