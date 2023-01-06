(ns tech.v3.datatype.ffi.nio-buf-mmodel-jdk19
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [java.lang.foreign MemoryAddress MemorySegment MemorySession]
           [java.nio ByteBuffer ByteOrder]))



(set! *warn-on-reflection* true)


(defn direct-buffer-constructor
  ^ByteBuffer [nbuf ^long address ^long nbytes _options]
  (let [retval (if-not (== 0 nbytes)
                 (-> (MemoryAddress/ofLong address)
                     (MemorySegment/ofAddress nbytes (MemorySession/global))
                     (.asByteBuffer))
                 (ByteBuffer/allocateDirect 0))
        endianness (dtype-proto/endianness nbuf)]
    (case endianness
      :little-endian (.order retval ByteOrder/LITTLE_ENDIAN)
      :big-endian (.order retval ByteOrder/BIG_ENDIAN))
    retval))
