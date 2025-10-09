(ns tech.v3.datatype.ffi.nio-buf-mmodel-jdk
  "JDK FFM API NIO ByteBuffer support - finalized in JDK 22 (JEP 454), available in LTS starting with JDK 25."
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [java.lang.foreign MemorySegment]
           [java.nio ByteBuffer ByteOrder]))



(set! *warn-on-reflection* true)


(defn direct-buffer-constructor
  ^ByteBuffer [nbuf ^long address ^long nbytes _options]
  (let [retval (if-not (== 0 nbytes)
                 (-> (MemorySegment/ofAddress address)
                     (.reinterpret nbytes)
                     (.asByteBuffer))
                 (ByteBuffer/allocateDirect 0))
        endianness (dtype-proto/endianness nbuf)]
    (case endianness
      :little-endian (.order retval ByteOrder/LITTLE_ENDIAN)
      :big-endian (.order retval ByteOrder/BIG_ENDIAN))
    retval))
