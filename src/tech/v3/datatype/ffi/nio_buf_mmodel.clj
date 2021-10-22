(ns tech.v3.datatype.ffi.nio-buf-mmodel
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [jdk.incubator.foreign MemoryAddress Addressable MemorySegment
            ResourceScope]
           [java.nio ByteBuffer ByteOrder]))


(set! *warn-on-reflection* true)


(defn direct-buffer-constructor
  ^ByteBuffer [nbuf ^long address ^long nbytes options]
  (let [retval (if-not (== 0 nbytes)
                 (-> (MemoryAddress/ofLong address)
                     (.asSegment nbytes (ResourceScope/globalScope))
                     (.asByteBuffer))
                 (ByteBuffer/allocateDirect 0))
        endianness (dtype-proto/endianness nbuf)]
    (case endianness
      :little-endian (.order retval ByteOrder/LITTLE_ENDIAN)
      :big-endian (.order retval ByteOrder/BIG_ENDIAN))
    retval))
