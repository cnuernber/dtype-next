(ns tech.v3.datatype.ffi.nio-buf-mmodel
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [jdk.incubator.foreign MemoryAddress Addressable MemorySegment
            NativeScope]
           [java.nio ByteBuffer ByteOrder]))


(set! *warn-on-reflection* true)


(defn direct-buffer-constructor
  ^ByteBuffer [nbuf ^long address ^long nbytes options]
  (let [retval (-> (MemoryAddress/ofLong address)
                   (.asSegmentRestricted nbytes nil nbuf)
                   (.share)
                   (.asByteBuffer))
        endianness (dtype-proto/endianness nbuf)]
    (case endianness
      :little-endian (.order retval ByteOrder/LITTLE_ENDIAN)
      :big-endian (.order retval ByteOrder/BIG_ENDIAN))
    retval))
