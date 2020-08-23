(ns tech.v3.datatype.native-buffer
  (:require [tech.resource :as resource]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.parallel.for :as parallel-for]
            [primitive-math :as pmath])
  (:import [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe]))

(set! *warn-on-reflection* true)


(defn unsafe
  ^Unsafe []
  UnsafeUtil/unsafe)


(defmacro native-buffer->io
  [datatype advertised-datatype buffer address n-elems]
  (let [byte-width (casting/numeric-byte-width datatype)]
    `(reify
       dtype-proto/PToNativeBuffer
       (convertible-to-native-buffer? [this#] true)
       (->native-buffer [this#] ~buffer)
       ;;Forward protocol methods that are efficiently implemented by the buffer
       dtype-proto/PBuffer
       (sub-buffer [this# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->reader {})))
       ~(typecast/datatype->reader-type (casting/safe-flatten datatype))
       (elemwiseDatatype [rdr#] ~advertised-datatype)
       (lsize [rdr#] ~n-elems)
       (read [rdr# ~'idx]
         ~(case datatype
            :int8 `(.getByte (unsafe) (pmath/+ ~address ~'idx))
            :uint8 `(-> (.getByte (unsafe) (pmath/+ ~address ~'idx))
                        (pmath/byte->ubyte))
            :int16 `(.getShort (unsafe) (pmath/+ ~address
                                                 (pmath/* ~'idx ~byte-width)))
            :uint16 `(-> (.getShort (unsafe) (pmath/+ ~address
                                                      (pmath/* ~'idx ~byte-width)))
                         (pmath/short->ushort))
            :char `(-> (.getShort (unsafe) (pmath/+ ~address
                                                    (pmath/* ~'idx ~byte-width)))
                       (RT/uncheckedCharCast))
            :int32 `(.getInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width)))
            :uint32 `(-> (.getInt (unsafe) (pmath/+ ~address
                                                    (pmath/* ~'idx ~byte-width)))
                         (pmath/int->uint))
            :int64 `(.getLong (unsafe) (pmath/+ ~address
                                                (pmath/* ~'idx ~byte-width)))
            :uint64 `(-> (.getLong (unsafe) (pmath/+ ~address
                                                     (pmath/* ~'idx ~byte-width))))
            :float32 `(.getFloat (unsafe) (pmath/+ ~address
                                                   (pmath/* ~'idx ~byte-width)))
            :float64 `(.getDouble (unsafe) (pmath/+ ~address
                                                    (pmath/* ~'idx ~byte-width)))))
       ~(typecast/datatype->writer-type (casting/safe-flatten datatype))
       (write [rdr# ~'idx ~'value]
         ~(case datatype
            :int8 `(.putByte (unsafe) (pmath/+ ~address ~'idx) ~'value)
            :uint8 `(.putByte (unsafe) (pmath/+ ~address ~'idx)
                              (casting/datatype->cast-fn :int16 :uint8 ~'value))
            :int16 `(.putShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                               ~'value)
            :uint16 `(.putShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                                (casting/datatype->cast-fn :int32 :uint16 ~'value))
            :char `(.putShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                              (unchecked-short ~'value))
            :int32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                             ~'value)
            :uint32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                              (casting/datatype->cast=fn :int64 :uint32 ~'value))
            :int64 `(.putLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                              ~'value)
            :uint64 `(.putLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                               ~'value)
            :float32 `(.putFloat (unsafe)
                                 (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                                 ~'value)
            :float64 `(.putDouble (unsafe)
                                  (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                                  ~'value))))))

(declare native-buffer->io)


;;Size is in elements, not in bytes
(defrecord NativeBuffer [^long address ^long n-elems datatype]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] this)
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [this] datatype)
  dtype-proto/PECount
  (ecount [this] n-elems)
  dtype-proto/PBuffer
  (sub-buffer [this offset length]
    (let [offset (long offset)
          length (long length)]
      (when-not (<= (+ offset length) n-elems)
        (throw (Exception.
                (format "Offset+length (%s) > n-elems (%s)"
                        (+ offset length) n-elems))))
      (NativeBuffer. (+ address offset) length datatype)))
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this options]
    (native-buffer->io this))
  dtype-proto/PToWriter
  (convertible-to-writer? [this] true)
  (->writer [this options]
    (native-buffer->io this)))


(defn- native-buffer->io
  [^NativeBuffer this]
  (let [datatype (.datatype this)
        address (.address this)
        n-elems (.n-elems this)]
    (case (casting/un-alias-datatype datatype)
      :int8 (native-buffer->io :int8 datatype this address n-elems)
      :uint8 (native-buffer->io :uint8 datatype this address n-elems)
      :int16 (native-buffer->io :int16 datatype this address n-elems)
      :uint16 (native-buffer->io :uint16 datatype this address n-elems)
      :char (native-buffer->io :char datatype this address n-elems)
      :int32 (native-buffer->io :int32 datatype this address n-elems)
      :uint32 (native-buffer->io :uint32 datatype this address n-elems)
      :int64 (native-buffer->io :int64 datatype this address n-elems)
      :uint64 (native-buffer->io :uint64 datatype this address n-elems)
      :float32 (native-buffer->io :float32 datatype this address n-elems)
      :float64 (native-buffer->io :float64 datatype this address n-elems))))


(defn as-native-buffer
  ^NativeBuffer [item]
  (when (dtype-proto/convertible-to-native-buffer? item)
    (dtype-proto/->native-buffer item)))


(defn native-buffer-byte-len
  ^long [^NativeBuffer nb]
  (let [original-size (.n-elems nb)]
    (* original-size (casting/numeric-byte-width
                      (dtype-proto/elemwise-datatype nb)))))


(defn set-native-datatype
  ^NativeBuffer [item datatype]
  (if-let [nb (as-native-buffer item)]
    (let [original-size (.n-elems nb)
          n-bytes (* original-size (casting/numeric-byte-width
                                    (dtype-proto/elemwise-datatype item)))
          new-byte-width (casting/numeric-byte-width
                          (casting/un-alias-datatype datatype))]
      (NativeBuffer. (.address nb) (quot n-bytes new-byte-width) datatype))))



;;One off data reading
(defn read-double
  (^double [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 8) 0))
   (.getDouble (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 8) 0))
   (.getDouble (unsafe) (.address native-buffer))))


(defn read-float
  (^double [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 4) 0))
   (.getFloat (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 4) 0))
   (.getFloat (unsafe) (.address native-buffer))))


(defn read-long
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 8) 0))
   (.getLong (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 8) 0))
   (.getLong (unsafe) (.address native-buffer))))


(defn read-int
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 4) 0))
   (.getInt (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 4) 0))
   (.getInt (unsafe) (.address native-buffer))))


(defn read-short
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 2) 0))
   (unchecked-long
    (.getShort (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 2) 0))
   (unchecked-long
    (.getShort (unsafe) (.address native-buffer)))))


(defn read-byte
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 1) 0))
   (unchecked-long
    (.getByte (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 1) 0))
   (unchecked-long
    (.getByte (unsafe) (.address native-buffer)))))



(defn free
  [data]
  (let [addr (long (if (instance? NativeBuffer data)
                     (.address ^NativeBuffer data)
                     (long data)))]
    (when-not (== 0 addr)
      (.freeMemory (unsafe) addr))))


(defn malloc
  (^NativeBuffer [^long n-bytes {:keys [resource-type]
                                 :or {resource-type :gc}}]
   (let [retval (NativeBuffer. (.allocateMemory (unsafe) n-bytes)
                               n-bytes
                               :int8)
         addr (.address retval)]
     (when resource-type
       (resource/track retval #(free addr) resource-type))
     retval))
  (^NativeBuffer [^long n-bytes]
   (malloc n-bytes {})))
