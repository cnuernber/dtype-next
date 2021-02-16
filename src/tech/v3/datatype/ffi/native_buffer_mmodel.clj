(ns tech.v3.datatype.ffi.native-buffer-mmodel
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.casting :as casting])
  (:import [jdk.incubator.foreign MemorySegment MemoryAddress]
           [tech.v3.datatype.native_buffer NativeBuffer]))


(set! *warn-on-reflection* true)


(defn memory-segment->native-buffer
  (^NativeBuffer [^MemorySegment mseg options]
   (let [addr (->(.address mseg)
                 (.toRawLongValue))
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
        addr (MemoryAddress/ofLong (.address nbuf))
        n-bytes (* (dtype-proto/ecount nbuf)
                   (casting/numeric-byte-width
                    (dtype-proto/elemwise-datatype nbuf)))]
    (-> (.asSegmentRestricted addr n-bytes nil nbuf)
        (.share))))
