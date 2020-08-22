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


(defmacro native-buffer->reader
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
                                                    (pmath/* ~'idx ~byte-width))))))))


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
    (case (casting/un-alias-datatype datatype)
      :int8 (native-buffer->reader :int8 datatype this address n-elems)
      :uint8 (native-buffer->reader :uint8 datatype this address n-elems)
      :int16 (native-buffer->reader :int16 datatype this address n-elems)
      :uint16 (native-buffer->reader :uint16 datatype this address n-elems)
      :int32 (native-buffer->reader :int32 datatype this address n-elems)
      :uint32 (native-buffer->reader :uint32 datatype this address n-elems)
      :int64 (native-buffer->reader :int64 datatype this address n-elems)
      :uint64 (native-buffer->reader :uint64 datatype this address n-elems)
      :float32 (native-buffer->reader :float32 datatype this address n-elems)
      :float64 (native-buffer->reader :float64 datatype this address n-elems))))


(defn as-native-buffer
  ^NativeBuffer [item]
  (when (dtype-proto/convertible-to-native-buffer? item)
    (dtype-proto/->native-buffer item)))


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
   (assert (>= (- (.n-elems native-buffer) offset 8) 0))
   (.getDouble (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 8) 0))
   (.getDouble (unsafe) (.address native-buffer))))


(defn read-float
  (^double [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 4) 0))
   (.getFloat (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 4) 0))
   (.getFloat (unsafe) (.address native-buffer))))


(defn read-long
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 8) 0))
   (.getLong (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 8) 0))
   (.getLong (unsafe) (.address native-buffer))))


(defn read-int
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 4) 0))
   (.getInt (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 4) 0))
   (.getInt (unsafe) (.address native-buffer))))


(defn read-short
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 2) 0))
   (unchecked-long
    (.getShort (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 2) 0))
   (unchecked-long
    (.getShort (unsafe) (.address native-buffer)))))


(defn read-byte
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (.n-elems native-buffer) offset 1) 0))
   (unchecked-long
    (.getByte (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (.n-elems native-buffer) 1) 0))
   (unchecked-long
    (.getByte (unsafe) (.address native-buffer)))))


(defn- unpack-copy-item
  [item ^long item-off]
  (if (instance? NativeBuffer item)
    ;;no further offsetting required for native buffers
    [nil (+ item-off (.address ^NativeBuffer item))]
    (let [ary (:java-array item)
          ary-off (:offset item)]
      [ary (+ item-off ary-off
              (case (dtype-proto/elemwise-datatype ary)
                :boolean Unsafe/ARRAY_BOOLEAN_BASE_OFFSET
                :int8 Unsafe/ARRAY_BYTE_BASE_OFFSET
                :int16 Unsafe/ARRAY_SHORT_BASE_OFFSET
                :int32 Unsafe/ARRAY_INT_BASE_OFFSET
                :int64 Unsafe/ARRAY_LONG_BASE_OFFSET
                :float32 Unsafe/ARRAY_FLOAT_BASE_OFFSET
                :float64 Unsafe/ARRAY_DOUBLE_BASE_OFFSET))])))


(defn copy!
  "Src, dst *must* be same unaliased datatype and that datatype must be a primitive
  datatype.
  src must either be convertible to an array or to a native buffer.
  dst must either be convertible to an array or to a native buffer.
  Uses Unsafe/copyMemory under the covers *without* safePointPolling.
  Returns dst"
  ([src src-off dst dst-off n-elems]
   (let [src-dt (casting/host-flatten (dtype-proto/elemwise-datatype src))
         dst-dt (casting/host-flatten (dtype-proto/elemwise-datatype dst))
         src-ec (dtype-proto/ecount src)
         dst-ec (dtype-proto/ecount dst)
         src-off (long src-off)
         dst-off (long dst-off)
         n-elems (long n-elems)
         _ (when-not (>= (- src-ec src-off) n-elems)
             (throw (Exception. (format "Src ecount (%s) - src offset (^%s) is less than op elem count (%s)"
                                        src-ec src-off n-elems))))
         _ (when-not (>= (- dst-ec dst-off) n-elems)
             (throw (Exception. (format "Dst ecount (%s) - dst offset (^%s) is less than op elem count (%s)"
                                        dst-ec dst-off n-elems))))
         _ (when-not (= src-dt dst-dt)
             (throw (Exception. (format "src datatype (%s) != dst datatype (%s)"
                                        src-dt dst-dt))))]
     ;;Check if managed heap or native heap
     (let [src (or (dtype-proto/->array-buffer src)
                   (dtype-proto/->native-buffer src))
           dst (or (dtype-proto/->array-buffer dst)
                   (dtype-proto/->native-buffer dst))
           _ (when-not (and src dst)
               (throw (Exception.
                       "Src or dst are not convertible to arrays or native buffers")))
           [src src-off] (unpack-copy-item src src-off)
           [dst dst-off] (unpack-copy-item dst dst-off)]
       (if (< n-elems 1024)
         (.copyMemory (unsafe) src (long src-off) dst (long dst-off)
                      (* n-elems (casting/numeric-byte-width
                                  (casting/un-alias-datatype src-dt))))
         (parallel-for/indexed-map-reduce
          n-elems
          (fn [^long start-idx ^long group-len]
            (.copyMemory (unsafe)
                         src (+ (long src-off) start-idx)
                         dst (+ (long dst-off) start-idx)
                         (* group-len (casting/numeric-byte-width
                                       (casting/un-alias-datatype src-dt)))))))
       dst)))
  ([src dst n-elems]
   (copy! src 0 dst 0 n-elems))
  ([src dst]
   (let [src-ec (dtype-proto/ecount src)
         dst-ec (dtype-proto/ecount dst)]
     (when-not (== src-ec dst-ec)
       (throw (Exception. (format "src ecount (%s) != dst ecount (%s)"
                                  src-ec dst-ec))))
     (copy! src 0 dst 0 src-ec))))


(defn free
  [data]
  (let [addr (long (if (instance? NativeBuffer data)
                     (.address ^NativeBuffer data)
                     (long data)))]
    (when-not (== 0 addr)
      (.freeMemory (unsafe) addr))))


(defn malloc
  (^NativeBuffer [^long n-bytes {:keys [resource-type]
                                 :or {resource-type :stack}}]
   (let [retval (NativeBuffer. (.allocateMemory (unsafe) n-bytes)
                               n-bytes
                               :int8)
         addr (.address retval)]
     (when resource-type
       (resource/track retval #(free addr) resource-type))
     retval))
  (^NativeBuffer [^long n-bytes]
   (malloc n-bytes {})))
