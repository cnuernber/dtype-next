(ns tech.v3.datatype.native-buffer
  (:require [tech.resource :as resource]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.parallel.for :as parallel-for]
            [primitive-math :as pmath])
  (:import [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe]
           [tech.v3.datatype Buffer BufferCollection]
           [clojure.lang RT IObj Counted Indexed IFn]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn unsafe
  ^Unsafe []
  UnsafeUtil/unsafe)


(defn- normalize-track-type
  [track-type]
  ;;default track type is gc for native buffers
  (let [track-type (or track-type :gc)]
    (if (keyword? track-type)
      #{track-type}
      (set track-type))))


(declare chain-native-buffers)

(defmacro read-value
  [address swap? datatype byte-width n-elems]
  `(do
     (errors/check-idx ~'idx ~n-elems)
     ~(if (not swap?)
       (case datatype
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
                                                 (pmath/* ~'idx ~byte-width))))
       (case datatype
         :int8 `(.getByte (unsafe) (pmath/+ ~address ~'idx))
         :uint8 `(-> (.getByte (unsafe) (pmath/+ ~address ~'idx))
                     (pmath/byte->ubyte))
         :int16 `(Short/reverseBytes
                  (.getShort (unsafe) (pmath/+ ~address
                                               (pmath/* ~'idx ~byte-width))))
         :uint16 `(-> (.getShort (unsafe) (pmath/+ ~address
                                                   (pmath/* ~'idx ~byte-width)))
                      (Short/reverseBytes)
                      (pmath/short->ushort))
         :char `(-> (.getShort (unsafe) (pmath/+ ~address
                                                 (pmath/* ~'idx ~byte-width)))
                    (Short/reverseBytes)
                    (RT/uncheckedCharCast))
         :int32 `(-> (.getInt (unsafe) (pmath/+ ~address
                                                (pmath/* ~'idx ~byte-width)))
                     (Integer/reverseBytes))
         :uint32 `(-> (.getInt (unsafe) (pmath/+ ~address
                                                 (pmath/* ~'idx ~byte-width)))
                      (Integer/reverseBytes)
                      (pmath/int->uint))
         :int64 `(-> (.getLong (unsafe) (pmath/+ ~address
                                                 (pmath/* ~'idx ~byte-width)))
                     (Long/reverseBytes))
         :uint64 `(-> (.getLong (unsafe) (pmath/+ ~address
                                                  (pmath/* ~'idx ~byte-width)))
                      (Long/reverseBytes))
         :float32 `(-> (.getInt (unsafe) (pmath/+ ~address
                                                  (pmath/* ~'idx ~byte-width)))
                       (Integer/reverseBytes)
                       (Float/intBitsToFloat))
         :float64 `(-> (.getLong (unsafe) (pmath/+ ~address
                                                   (pmath/* ~'idx ~byte-width)))
                       (Long/reverseBytes)
                       (Double/longBitsToDouble))))))

(defmacro write-value
  [address swap? datatype byte-width n-elems]
  `(do
     (errors/check-idx ~'idx ~n-elems)
     ~(if (not swap?)
       (case datatype
         :int8 `(.putByte (unsafe) (pmath/+ ~address ~'idx) ~'value)
         :uint8 `(.putByte (unsafe) (pmath/+ ~address ~'idx)
                           (casting/datatype->cast-fn :int16 :uint8 ~'value))
         :int16 `(.putShort (unsafe) (pmath/+ ~address
                                              (pmath/* ~'idx ~byte-width))
                            ~'value)
         :uint16 `(.putShort (unsafe) (pmath/+ ~address
                                               (pmath/* ~'idx ~byte-width))
                             (casting/datatype->cast-fn :int32 :uint16 ~'value))
         :char `(.putShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                           (unchecked-short (int ~'value)))
         :int32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                          ~'value)
         :uint32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                           (casting/datatype->cast-fn :int64 :uint32 ~'value))
         :int64 `(.putLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                           ~'value)
         :uint64 `(.putLong (unsafe) (pmath/+ ~address
                                              (pmath/* ~'idx ~byte-width))
                            ~'value)
         :float32 `(.putFloat (unsafe)
                              (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                              ~'value)
         :float64 `(.putDouble (unsafe)
                               (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                               ~'value))
       (case datatype
         :int8 `(.putByte (unsafe) (pmath/+ ~address ~'idx) ~'value)
         :uint8 `(.putByte (unsafe) (pmath/+ ~address ~'idx)
                           (casting/datatype->cast-fn :int16 :uint8 ~'value))
         :int16 `(.putShort (unsafe) (pmath/+ ~address
                                              (pmath/* ~'idx ~byte-width))
                            (Short/reverseBytes ~'value))
         :uint16 `(.putShort (unsafe) (pmath/+ ~address
                                               (pmath/* ~'idx ~byte-width))
                             (Short/reverseBytes (casting/datatype->cast-fn
                                                  :int32 :uint16 ~'value)))
         :char `(.putShort (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                           (Short/reverseBytes (unchecked-short (int ~'value))))
         :int32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                          (Integer/reverseBytes ~'value))
         :uint32 `(.putInt (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                           (Integer/reverseBytes (casting/datatype->cast-fn
                                                  :int64 :uint32 ~'value)))
         :int64 `(.putLong (unsafe) (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                           (Long/reverseBytes ~'value))
         :uint64 `(.putLong (unsafe) (pmath/+ ~address
                                              (pmath/* ~'idx ~byte-width))
                            (Long/reverseBytes ~'value))
         :float32 `(.putInt (unsafe)
                            (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                            (-> (Float/floatToIntBits ~'value)
                                (Integer/reverseBytes)))
         :float64 `(.putLong (unsafe)
                             (pmath/+ ~address (pmath/* ~'idx ~byte-width))
                             (-> ~'value
                                 (Double/doubleToLongBits)
                                 (Long/reverseBytes)))))))


(defmacro native-buffer->buffer-macro
  [datatype advertised-datatype buffer address n-elems swap?]
  (let [byte-width (casting/numeric-byte-width datatype)]
    `(let [{~'unpacking-read :unpacking-read
            ~'packing-write :packing-write} (packing/buffer-packing-pair ~advertised-datatype)]
      (reify
              dtype-proto/PToNativeBuffer
              (convertible-to-native-buffer? [this#] true)
              (->native-buffer [this#] ~buffer)
              dtype-proto/PEndianness
              (endianness [item] (dtype-proto/endianness ~buffer))
              ;;Forward protocol methods that are efficiently implemented by the buffer
              dtype-proto/PSubBuffer
              (sub-buffer [this# offset# length#]
                (-> (dtype-proto/sub-buffer ~buffer offset# length#)
                    (dtype-proto/->reader)))
              ~(typecast/datatype->io-type (casting/safe-flatten datatype))
              (elemwiseDatatype [rdr#] ~advertised-datatype)
              (lsize [rdr#] ~n-elems)
              (allowsRead [rdr#] true)
              (allowsWrite [rdr#] true)
              ~@(cond
                  (= datatype :boolean)
                  [`(readBoolean [rdr# ~'idx]
                                 (read-value ~address ~swap? ~datatype ~byte-width ~n-elems))]
                  ;;For integer types, everything implements readlong.
                  ;;They also implement readX where X maps to exactly the datatype.
                  ;;For example byte arrays implement readLong and readByte.
                  (casting/integer-type? datatype)
                  (concat
                   [`(readLong [rdr# ~'idx]
                               (casting/datatype->unchecked-cast-fn
                                ~datatype :int64
                                (read-value ~address ~swap? ~datatype ~byte-width ~n-elems)))]
                   (when-not (= :int64 (casting/safe-flatten datatype))
                     ;;Exact reader fns for the exact datatype
                     [(cond
                        (= datatype :int8)
                        `(readByte [rdr# ~'idx]
                                   (read-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        (= (casting/safe-flatten datatype) :int16)
                        `(readShort [rdr# ~'idx]
                                    (read-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        (= datatype :char)
                        `(readChar [rdr# ~'idx]
                                   (read-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        (= (casting/safe-flatten datatype) :int32)
                        `(readInt [rdr# ~'idx]
                                  (read-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        :else (throw (Exception. (format "Macro expansion error-%s"
                                                         datatype))))])
                   (if (= :char datatype)
                     [`(readObject [rdr# ~'idx]
                                   (.readChar rdr# ~'idx))]
                     ;;Integer types may be representing packed objects
                     [`(readObject [~'rdr ~'idx]
                                   (if ~'unpacking-read
                                     (~'unpacking-read ~'rdr ~'idx)
                                     (.readLong ~'rdr ~'idx)))]))
                  (casting/float-type? datatype)
                  [`(readDouble [rdr# ~'idx]
                                (casting/datatype->unchecked-cast-fn
                                 ~datatype :float64
                                 (read-value ~address ~swap? ~datatype ~byte-width ~n-elems)))
                   `(readFloat [rdr# ~'idx]
                               (casting/datatype->unchecked-cast-fn
                                ~datatype :float32
                                (read-value ~address ~swap? ~datatype ~byte-width ~n-elems)))]
                  :else
                  [`(readObject [rdr# ~'idx]
                                (read-value ~address ~swap? ~datatype ~byte-width ~n-elems))])
              ~@(cond
                  (= :boolean datatype)
                  [`(writeBoolean [wtr# idx# ~'value]
                                  (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))]
                  (casting/integer-type? datatype)
                  (concat
                   [`(writeLong [rdr# ~'idx ~'value]
                                (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))]
                   (when-not (= :int64 (casting/safe-flatten datatype))
                     ;;Exact reader fns for the exact datatype
                     [(cond
                        (= datatype :int8)
                        `(writeByte [rdr# ~'idx ~'value]
                                    (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        (= (casting/safe-flatten datatype) :int16)
                        `(writeShort [rdr# ~'idx ~'value]
                                     (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        (= datatype :char)
                        `(writeChar [rdr# ~'idx ~'value]
                                    (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        (= (casting/safe-flatten datatype) :int32)
                        `(writeInt [rdr# ~'idx ~'value]
                                   (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                        :else (throw (Exception. (format "Macro expansion error-%s"
                                                         datatype))))])
                   (if (= :char datatype)
                     [`(writeObject [rdr# ~'idx ~'value]
                                    (.writeChar rdr# ~'idx (char ~'value)))]
                     [`(writeObject [~'rdr ~'idx ~'value]
                             (if ~'packing-write
                               (~'packing-write ~'rdr ~'idx ~'value)
                               (.writeLong ~'rdr ~'idx (long ~'value))))]))
                  (casting/float-type? datatype)
                  [`(writeDouble [rdr# ~'idx ~'value]
                                 (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))
                   `(writeFloat [rdr# ~'idx ~'value]
                                (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))]
                  :else
                  [`(writeObject [wtr# idx# val#]
                                 ;;Writing values is always checked, no options.
                                 (write-value ~address ~swap? ~datatype ~byte-width ~n-elems))])))))


(declare native-buffer->buffer)


;;Size is in elements, not in bytes
(deftype NativeBuffer [^long address ^long n-elems datatype endianness
                       resource-type metadata
                       ^:volatile-mutable ^Buffer cached-io]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] this)
  dtype-proto/PEndianness
  (endianness [item] endianness)
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [this] datatype)
  dtype-proto/PECount
  (ecount [this] n-elems)
  dtype-proto/PSubBuffer
  (sub-buffer [this offset length]
    (let [byte-width (casting/numeric-byte-width datatype)
          offset (long offset)
          length (long length)]
      (when-not (<= (+ offset length) n-elems)
        (throw (Exception.
                (format "Offset+length (%s) > n-elems (%s)"
                        (+ offset length) n-elems))))
      (chain-native-buffers this
                            (NativeBuffer. (+ address (* offset byte-width))
                                           length datatype endianness
                                           resource-type metadata nil))))
  dtype-proto/PSetConstant
  (set-constant! [this offset element-count value]
    (let [offset (long offset)
          value (casting/cast value datatype)
          element-count (long element-count)
          byte-width (casting/numeric-byte-width datatype)
          address (+ address (* offset byte-width))
          n-bytes (* element-count byte-width)]
      (when-not (<= (+ offset element-count) n-elems)
        (throw (Exception. (format
                            "Attempt to set constant value out of range: %s+%s >= %s"
                            offset element-count n-elems))))
      (if (and (number? value)
               (or (== 0.0 (double value))
                   (= datatype :int8)))
        (.setMemory (unsafe) address n-bytes (unchecked-byte value))
        (let [^Buffer writer (-> (dtype-proto/sub-buffer this offset
                                                         element-count)
                                 (dtype-proto/->writer))]
          (parallel-for/parallel-for
           idx element-count
           (.writeObject writer idx value))))
      this))
  dtype-proto/PClone
  (clone [this]
    (dtype-proto/make-container :native-heap datatype this
                                {:endianness endianness
                                 :resource-type resource-type}))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [this] true)
  (->buffer [this]
    (if cached-io
      cached-io
      (do
        (set! cached-io (native-buffer->buffer this))
        cached-io)))
  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this]
    (dtype-proto/->buffer this))
  dtype-proto/PToWriter
  (convertible-to-writer? [this] true)
  (->writer [this]
    (dtype-proto/->buffer this))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (NativeBuffer. address n-elems datatype endianness resource-type
                   metadata
                   cached-io))
  Counted
  (count [item] (int (dtype-proto/ecount item)))
  Indexed
  (nth [item idx]
    (errors/check-idx idx n-elems)
    ((dtype-proto/->buffer item) idx))
  (nth [item idx def-val]
    (if (and (>= idx 0) (< idx (.count item)))
      ((dtype-proto/->buffer item) idx)
      def-val))
  IFn
  (invoke [item idx]
    (.nth item (int idx)))
  (invoke [item idx value]
    (let [idx (long idx)]
      (errors/check-idx idx n-elems)
      ((dtype-proto/->writer item) idx value)))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  BufferCollection
  (iterator [this]
    (dtype-proto/->buffer this)
    (.iterator cached-io))
  (size [this] (int (dtype-proto/ecount this)))
  (toArray [this]
    (dtype-proto/->buffer this)
    (.toArray cached-io))
  Object
  (toString [buffer]
    (dtype-pp/buffer->string buffer (format "native-buffer@0x%016X"
                                            (.address buffer)))))


(dtype-pp/implement-tostring-print NativeBuffer)


(defn- native-buffer->buffer
  [^NativeBuffer this]
  (let [datatype (.datatype this)
        address (.address this)
        n-elems (.n-elems this)
        swap? (not= (.endianness this) (dtype-proto/platform-endianness))]
    (if swap?
      (case (casting/un-alias-datatype datatype)
        :int8 (native-buffer->buffer-macro :int8 datatype this address n-elems true)
        :uint8 (native-buffer->buffer-macro :uint8 datatype this address n-elems true)
        :int16 (native-buffer->buffer-macro :int16 datatype this address n-elems true)
        :uint16 (native-buffer->buffer-macro :uint16 datatype this address n-elems true)
        :char (native-buffer->buffer-macro :char datatype this address n-elems true)
        :int32 (native-buffer->buffer-macro :int32 datatype this address n-elems true)
        :uint32 (native-buffer->buffer-macro :uint32 datatype this address n-elems true)
        :int64 (native-buffer->buffer-macro :int64 datatype this address n-elems true)
        :uint64 (native-buffer->buffer-macro :uint64 datatype this address n-elems true)
        :float32 (native-buffer->buffer-macro :float32 datatype this address n-elems true)
        :float64 (native-buffer->buffer-macro :float64 datatype this address n-elems true))
      (case (casting/un-alias-datatype datatype)
        :int8 (native-buffer->buffer-macro :int8 datatype this address n-elems false)
        :uint8 (native-buffer->buffer-macro :uint8 datatype this address n-elems false)
        :int16 (native-buffer->buffer-macro :int16 datatype this address n-elems false)
        :uint16 (native-buffer->buffer-macro :uint16 datatype this address n-elems false)
        :char (native-buffer->buffer-macro :char datatype this address n-elems false)
        :int32 (native-buffer->buffer-macro :int32 datatype this address n-elems false)
        :uint32 (native-buffer->buffer-macro :uint32 datatype this address n-elems false)
        :int64 (native-buffer->buffer-macro :int64 datatype this address n-elems false)
        :uint64 (native-buffer->buffer-macro :uint64 datatype this address n-elems false)
        :float32 (native-buffer->buffer-macro :float32 datatype this address n-elems false)
        :float64 (native-buffer->buffer-macro :float64 datatype this
                                              address n-elems false)))))


(defn- chain-native-buffers
  [^NativeBuffer old-buf ^NativeBuffer new-buf]
  ;;If the resource type is GC, we have to associate the new buf with the old buf
  ;;such that the old buffer can't get cleaned up while the new buffer is still
  ;;referencable via the gc.
  (if ((.resource-type old-buf) :gc)
    (resource/track new-buf #(constantly old-buf) :gc)
    new-buf))


(defn- validate-endianness
  [endianness]
  (when-not (#{:little-endian :big-endian} endianness)
    (throw (Exception. (format "Unrecognized endianness: %s" endianness))))
  endianness)


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
  (let [nb (as-native-buffer item)
        original-size (.n-elems nb)
        n-bytes (* original-size (casting/numeric-byte-width
                                  (dtype-proto/elemwise-datatype item)))
        new-byte-width (casting/numeric-byte-width
                        (casting/un-alias-datatype datatype))]
    (chain-native-buffers
     item
     (NativeBuffer. (.address nb) (quot n-bytes new-byte-width)
                    datatype (.endianness nb)
                    (.resource-type nb) nil nil))))


(defn set-endianness
  ^NativeBuffer [item endianness]
  (let [nb (as-native-buffer item)]
    (validate-endianness endianness)
    (if (= endianness (.endianness nb))
      nb
      (chain-native-buffers item
                            (NativeBuffer. (.address nb) (.n-elems nb)
                                           (.datatype nb) endianness
                                           (.resource-type nb) nil nil)))))


(defn native-buffer->map
  [^NativeBuffer buf]
  {:address (.address buf)
   :length (.n-elems buf)
   :byte-length (* (.n-elems buf) (casting/numeric-byte-width (.datatype buf)))
   :datatype (.datatype buf)
   :resource-type (.resource-type buf)
   :endianness (.endianness buf)
   :metadata (.metadata buf)})


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
  (^NativeBuffer [^long n-bytes {:keys [resource-type uninitialized?
                                        endianness]
                                 :or {resource-type :gc}}]
   (let [resource-type (normalize-track-type resource-type)
         endianness (-> (or endianness (dtype-proto/platform-endianness))
                        (validate-endianness))
         retval (NativeBuffer. (.allocateMemory (unsafe) n-bytes)
                               n-bytes
                               :int8
                               endianness
                               resource-type nil nil)
         addr (.address retval)]
     (when-not uninitialized?
       (.setMemory (unsafe) addr n-bytes 0))
     (when resource-type
       (resource/track retval #(free addr) resource-type))
     retval))
  (^NativeBuffer [^long n-bytes]
   (malloc n-bytes {})))


(defn wrap-address
  ^NativeBuffer [address n-bytes datatype endianness gc-obj]
  (let [byte-width (casting/numeric-byte-width datatype)
        retval (NativeBuffer. address (quot (long n-bytes) byte-width)
                              datatype endianness #{:gc} nil nil)]
    ;;when we have to chain this to the gc objects
    (when gc-obj
      (resource/track retval (constantly gc-obj) :gc))
    retval))
