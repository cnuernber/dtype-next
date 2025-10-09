(ns tech.v3.datatype.ffi.native-buffer-mmodel-jdk
  "JDK FFM API MemorySegment support - finalized in JDK 22 (JEP 454), available in LTS starting with JDK 25."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.casting :as casting])
  (:import [java.lang.foreign MemorySegment]
           [tech.v3.datatype.native_buffer NativeBuffer]))


(set! *warn-on-reflection* true)


(defn memory-segment->native-buffer
  (^NativeBuffer [^MemorySegment mseg options]
   (let [addr (.address mseg)
         endianness (:endianness options (dtype-proto/platform-endianness))]
     (native-buffer/wrap-address addr (.byteSize mseg) :int8 endianness mseg)))
  (^NativeBuffer [mseg]
   (memory-segment->native-buffer mseg nil)))


(extend-type MemorySegment
  dtype-proto/PDatatype
  (datatype [item] :memory-segment)
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [item] :int8)
  dtype-proto/PECount
  (ecount [item] (.byteSize item))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [item] true)
  (->native-buffer [item] (memory-segment->native-buffer item)))


(defn native-buffer->memory-segment
  ^MemorySegment [item]
  (errors/when-not-errorf
   (dtype-proto/convertible-to-native-buffer? item)
   "Item type (%s) is not convertible to native buffer"
   (type item))
  (let [^NativeBuffer nbuf (dtype-proto/->native-buffer item)
        n-bytes (* (dtype-proto/ecount nbuf)
                   (casting/numeric-byte-width
                    (dtype-proto/elemwise-datatype nbuf)))]
    (-> (MemorySegment/ofAddress (.-address nbuf))
        (.reinterpret n-bytes))))
