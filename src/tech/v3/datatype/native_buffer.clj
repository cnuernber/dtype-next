(ns tech.v3.datatype.native-buffer
  "Support for malloc/free and generalized support for reading/writing typed data
  to a long integer address of memory."
  (:require [tech.v3.resource :as resource]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.graal-native :as graal-native]
            [tech.v3.parallel.for :as parallel-for]
            [clojure.tools.logging :as log]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype UnsafeUtil]
           [sun.misc Unsafe]
           [tech.v3.datatype Buffer BufferCollection BinaryBuffer]
           [clojure.lang RT IObj Counted Indexed IFn]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(graal-native/when-not-defined-graal-native
 (require '[clojure.pprint :as pp]))


(defn unsafe
  "Get access to an instance of sun.misc.Unsafe."
  ^Unsafe []
  UnsafeUtil/unsafe)


(defmacro ^:private read-value
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

(defmacro ^:private write-value
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


(defmacro ^:private accum-value
  [address swap? datatype byte-width n-elems]
  (let [host-dtype (casting/host-flatten datatype)
        [write-fn read-fn] (case host-dtype
                             :int8 ['.putByte '.getByte]
                             :int16 ['.putShort '.getShort]
                             :char ['.putShort '.getShort]
                             :int32 ['.putInt '.getInt]
                             :int64 ['.putLong '.getLong]
                             :float32 ['.putFloat '.getFloat]
                             :float64 ['.putDouble '.getDouble])
        accum-type (if (casting/integer-type? datatype)
                     :int64
                     :float64)]
    `(do
       ;;we have to check here
       (errors/check-idx ~'idx ~n-elems)
       (let [~'addr (pmath/+ ~address (pmath/* ~'idx ~byte-width))
             ~'unsafe (unsafe)]
         ~(if (not swap?)
            `(~write-fn ~'unsafe ~'addr
              (->> (pmath/+ ~'value (casting/datatype->cast-fn
                                     ~datatype ~accum-type
                                     (~read-fn ~'unsafe ~'addr)))
                   (casting/datatype->cast-fn ~accum-type ~datatype)
                   (casting/datatype->cast-fn ~datatype ~host-dtype)))
            (let [read-swap (case host-dtype
                              :int8 `(.getByte ~'unsafe ~'addr)
                              :int16 `(-> (.getShort ~'unsafe ~'addr)
                                          (Short/reverseBytes))
                              :char `(-> (.getShort ~'unsafe ~'addr)
                                         (Short/reverseBytes))
                              :int32 `(-> (.getInt ~'unsafe ~'addr)
                                          (Integer/reverseBytes))
                              :int64 `(-> (.getLong ~'unsafe ~'addr)
                                          (Long/reverseBytes))
                              :float32 `(-> (.getInt ~'unsafe ~'addr)
                                            (Integer/reverseBytes)
                                            (Float/intBitsToFloat))
                              :float64 `(-> (.getLong ~'unsafe ~'addr)
                                            (Long/reverseBytes)
                                            (Double/longBitsToDouble)))
                  write-swap (case host-dtype
                               :int8 `(.putByte ~'unsafe ~'addr ~'value)
                               :int16 `(->> (Short/reverseBytes ~'value)
                                            (.putShort ~'unsafe ~'addr))
                               :char `(->> (Short/reverseBytes ~'value)
                                           (.putShort ~'unsafe ~'addr))
                               :int32 `(->>  (Integer/reverseBytes ~'value)
                                             (.putInt ~'unsafe ~'addr))
                               :int64 `(->> (Long/reverseBytes ~'value)
                                            (.putLong ~'unsafe ~'addr))
                               :float32 `(->> (Float/floatToIntBits ~'value)
                                              (Integer/reverseBytes)
                                              (.putInt ~'unsafe ~'addr))
                               :float64 `(->> (Double/doubleToLongBits ~'value)
                                              (Long/reverseBytes)
                                              (.putLong ~'unsafe ~'addr)))]
              `(let [~'value (->> (pmath/+ ~'value
                                           (casting/datatype->unchecked-cast-fn
                                            ~datatype ~accum-type
                                            ~read-swap))
                                  (casting/datatype->cast-fn ~accum-type ~datatype)
                                  (casting/datatype->cast-fn ~datatype ~host-dtype))]
                 ~write-swap)))))))


(defmacro ^:private native-buffer->buffer-macro
  [datatype advertised-datatype buffer address n-elems swap?]
  (let [byte-width (casting/numeric-byte-width datatype)]
    `(let [{~'unpacking-read :unpacking-read
            ~'packing-write :packing-write}
           (packing/buffer-packing-pair ~advertised-datatype)]
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
         dtype-proto/PDatatype
         (datatype [rdr#] (dtype-proto/datatype ~buffer))
         dtype-proto/PElemwiseReaderCast
         (elemwise-reader-cast [item# new-dtype#] item#)
         dtype-proto/PToBinaryBuffer
         (convertible-to-binary-buffer? [item#] true)
         (->binary-buffer [item#]
           (dtype-proto/->binary-buffer ~buffer))
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
                           (read-value ~address ~swap? ~datatype ~byte-width ~n-elems)))
               `(accumPlusLong
                 [rdr# ~'idx ~'value]
                 (accum-value ~address ~swap? ~datatype ~byte-width ~n-elems))]
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
                           (read-value ~address ~swap? ~datatype ~byte-width ~n-elems)))
              `(accumPlusDouble
                 [rdr# ~'idx ~'value]
                 (accum-value ~address ~swap? ~datatype ~byte-width ~n-elems))]
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


(declare native-buffer->buffer native-buffer->map construct-binary-buffer)


;;Size is in elements, not in bytes
(deftype NativeBuffer [^long address ^long n-elems datatype endianness
                       resource-type metadata
                       ^:volatile-mutable ^Buffer cached-io
                       parent]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] this)
  dtype-proto/PEndianness
  (endianness [item] endianness)
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [this] datatype)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype]
    (or cached-io (dtype-proto/->reader item)))
  dtype-proto/PDatatype
  (datatype [this] :native-buffer)
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
      (NativeBuffer. (+ address (* offset byte-width))
                     length datatype endianness
                     resource-type metadata nil
                     this)))
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
    (dtype-proto/make-container :native-heap datatype
                                {:endianness endianness
                                 :resource-type resource-type}
                                this))
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
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [buf] true)
  (->binary-buffer [buf] (construct-binary-buffer buf))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (NativeBuffer. address n-elems datatype endianness resource-type
                   metadata
                   cached-io
                   parent))
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
    (if-not (:record-print? metadata)
      (dtype-pp/buffer->string buffer (format "native-buffer@0x%016X"
                                              (.address buffer)))
      (with-out-str
        (graal-native/if-defined-graal-native
         (println (native-buffer->map buffer))
         (pp/pprint (native-buffer->map buffer)))))))


(dtype-pp/implement-tostring-print NativeBuffer)


(casting/add-object-datatype! :native-buffer NativeBuffer false)


(defn- native-buffer->buffer
  [^NativeBuffer this]
  (let [datatype (.elemwise-datatype this)
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


(defn- validate-endianness
  [endianness]
  (when-not (#{:little-endian :big-endian} endianness)
    (throw (Exception. (format "Unrecognized endianness: %s" endianness))))
  endianness)


(defn as-native-buffer
  "Convert a thing to a native buffer if possible.  Calls
  tech.v3.datatype.protocols/->native-buffer if object indicates it is convertible
  to a native buffer."
  ^NativeBuffer [item]
  (when (dtype-proto/convertible-to-native-buffer? item)
    (dtype-proto/->native-buffer item)))


(defn native-buffer-byte-len
  "Get the length, in bytes, of a native buffer."
  ^long [^NativeBuffer nb]
  (let [original-size (.n-elems nb)]
    (* original-size (casting/numeric-byte-width
                      (dtype-proto/elemwise-datatype nb)))))


(defn set-native-datatype
  "Set the datatype of a native buffer.  n-elems will be recalculated."
  ^NativeBuffer [item datatype]
  (let [nb (as-native-buffer item)
        original-size (.n-elems nb)
        n-bytes (* original-size (casting/numeric-byte-width
                                  (dtype-proto/elemwise-datatype item)))
        new-byte-width (casting/numeric-byte-width
                        (casting/un-alias-datatype datatype))]
    (NativeBuffer. (.address nb) (quot n-bytes new-byte-width)
                   datatype (.endianness nb)
                   (.resource-type nb) (meta nb) nil item)))


(defn set-endianness
  "Convert a native buffer to simple hashmap for printing or logging purposes."
  ^NativeBuffer [item endianness]
  (let [nb (as-native-buffer item)]
    (validate-endianness endianness)
    (if (= endianness (.endianness nb))
      nb
      (NativeBuffer. (.address nb) (.n-elems nb)
                     (.elemwise-datatype nb) endianness
                     (.resource-type nb) (meta nb) nil item))))


(defn native-buffer->map
  "Convert a native buffer to simple hashmap for printing or logging purposes."
  [^NativeBuffer buf]
  {:address (.address buf)
   :length (.n-elems buf)
   :byte-length (* (.n-elems buf) (casting/numeric-byte-width
                                   (.elemwise-datatype buf)))
   :datatype (.elemwise-datatype buf)
   :resource-type (.resource-type buf)
   :endianness (.endianness buf)
   :metadata (.metadata buf)})


;;One off data reading
(defn read-double
  "Ad-hoc read a double at a given offset from a native buffer.  This method is not endian-aware."
  (^double [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 8) 0))
   (.getDouble (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 8) 0))
   (.getDouble (unsafe) (.address native-buffer))))


(defn read-float
  "Ad-hoc read a float at a given offset from a native buffer.  This method is not endian-aware."
  (^double [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 4) 0))
   (.getFloat (unsafe) (+ (.address native-buffer) offset)))
  (^double [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 4) 0))
   (.getFloat (unsafe) (.address native-buffer))))


(defn read-long
  "Ad-hoc read a long at a given offset from a native buffer.  This method is not endian-aware."
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 8) 0))
   (.getLong (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 8) 0))
   (.getLong (unsafe) (.address native-buffer))))


(defn read-int
  "Ad-hoc read an integer at a given offset from a native buffer.  This method is not endian-aware."
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 4) 0))
   (.getInt (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 4) 0))
   (.getInt (unsafe) (.address native-buffer))))


(defn read-short
  "Ad-hoc read a short at a given offset from a native buffer.  This method is not endian-aware."
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 2) 0))
   (unchecked-long
    (.getShort (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 2) 0))
   (unchecked-long
    (.getShort (unsafe) (.address native-buffer)))))


(defn read-byte
  "Ad-hoc read a byte at a given offset from a native buffer."
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 1) 0))
   (unchecked-long
    (.getByte (unsafe) (+ (.address native-buffer) offset))))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 1) 0))
   (unchecked-long
    (.getByte (unsafe) (.address native-buffer)))))


(defn free
  "Free a long ptr.  Malloc will do this for you.  Calling this is probably a mistake."
  [data]
  (let [addr (long (if (instance? NativeBuffer data)
                     (.address ^NativeBuffer data)
                     (long data)))]
    (when-not (== 0 addr)
      (.freeMemory (unsafe) addr))))


(defn malloc
  "Malloc memory.  If a desired buffer type is needed follow up with set-native-datatype.

  Options:

  * `:resource-type` - defaults to `:gc` - maps to `:track-type` in `tech.v3.resource`
     but can also be set to nil in which case the data is not tracked this library will
     not clean it up.
  * `:uninitialized?` - do not initialize to zero.  Use for perf in very very rare cases.
  * `:endianness` - Either `:little-endian` or `:big-endian` - defaults to platform.
  * `:log-level` - one of `#{:debug :trace :info :warn :error :fatal}` or nil if no logging
     is desired.  When enabled allocations and frees will be logged in the same manner as
     `tech.jna`."
  (^NativeBuffer [^long n-bytes {:keys [resource-type uninitialized?
                                        endianness log-level]
                                 :or {resource-type :gc}}]
   (let [endianness (-> (or endianness (dtype-proto/platform-endianness))
                        (validate-endianness))
         retval (NativeBuffer. (.allocateMemory (unsafe) n-bytes)
                               n-bytes
                               :int8
                               endianness
                               resource-type nil nil nil)
         addr (.address retval)]
     (when log-level
       (log/logf log-level "Malloc - 0x%016X - %016d bytes" (.address retval) n-bytes))
     (when-not uninitialized?
       (.setMemory (unsafe) addr n-bytes 0))
     (when resource-type
       (resource/track retval {:dispose-fn #(do
                                              (when log-level
                                                (log/logf log-level
                                                          "Free   - 0x%016X - %016d bytes"
                                                          addr n-bytes))
                                              (free addr))
                               :track-type resource-type}))
     retval))
  (^NativeBuffer [^long n-bytes]
   (malloc n-bytes {})))


(defn wrap-address
  "Wrap a long interger address with a native buffer.  gc-obj, if provided
  will be linked to the native buffer such that gc-obj will not be garbage
  collected before native buffer is garbage collected."
  (^NativeBuffer [address n-bytes datatype endianness gc-obj]
   (errors/when-not-error
    (or (not= 0 (long address))
        (== 0 (long n-bytes)))
    "Attempt to wrap 0 as an address for a native buffer")
   (let [byte-width (casting/numeric-byte-width datatype)]
     (NativeBuffer. address (quot (long n-bytes) byte-width)
                    datatype endianness #{:gc} nil nil gc-obj)))
  (^NativeBuffer [address n-bytes gc-obj]
   (wrap-address address n-bytes :int8 (dtype-proto/platform-endianness)
                 gc-obj)))


(defmacro ^:private check-bounds
  [dt-width byte-offset address n-bytes]
  `(do
     (errors/when-not-errorf
      (pmath/<= (pmath/+ ~byte-offset ~dt-width) ~n-bytes)
      "Element access out of range - %d >= %d"
      ~byte-offset ~n-bytes)
     (pmath/+ ~address ~byte-offset)))


(deftype NativeBinaryBuffer [^NativeBuffer nbuf
                             ^long address
                             ^long n-bytes
                             metadata]
  dtype-proto/PClone
  (clone [this]
    (-> (dtype-proto/clone nbuf)
        (dtype-proto/->binary-buffer)))
  BinaryBuffer
  (lsize [this] n-bytes)
  (allowsBinaryRead [this] true)
  (readBinByte [this byteOffset]
    (.getByte (unsafe) (check-bounds 1 byteOffset address n-bytes)))
  (readBinShort [this byteOffset]
    (.getShort (unsafe) (check-bounds 2 byteOffset address n-bytes)))
  (readBinInt [this byteOffset]
    (.getInt (unsafe) (check-bounds 4 byteOffset address n-bytes)))
  (readBinLong [this byteOffset]
    (.getLong (unsafe) (check-bounds 8 byteOffset address n-bytes)))
  (readBinFloat [this byteOffset]
    (.getFloat (unsafe) (check-bounds 4 byteOffset address n-bytes)))
  (readBinDouble [this byteOffset]
    (.getDouble (unsafe) (check-bounds 8 byteOffset address n-bytes)))

  (allowsBinaryWrite [this] true)
  (writeBinByte [this byteOffset data]
    (.putByte (unsafe) (check-bounds 1 byteOffset address n-bytes) data))
  (writeBinShort [this byteOffset data]
    (.putShort (unsafe) (check-bounds 2 byteOffset address n-bytes) data))
  (writeBinInt [this byteOffset data]
    (.putInt (unsafe) (check-bounds 4 byteOffset address n-bytes) data))
  (writeBinLong [this byteOffset data]
    (.putLong (unsafe) (check-bounds 8 byteOffset address n-bytes) data))
  (writeBinFloat [this byteOffset data]
    (.putFloat (unsafe) (check-bounds 4 byteOffset address n-bytes) data))
  (writeBinDouble [this byteOffset data]
    (.putDouble (unsafe) (check-bounds 8 byteOffset address n-bytes) data))
  IObj
  (meta [this] metadata)
  (withMeta [this newMeta]
    (NativeBinaryBuffer. nbuf address n-bytes newMeta)))


(defn construct-binary-buffer
  ^BinaryBuffer [^NativeBuffer nbuf]
  (errors/when-not-error
   (= (dtype-proto/platform-endianness)
      (dtype-proto/endianness nbuf))
   "Endianness conversion is unsupported for binary buffers")
  (let [n-bytes (* (.n-elems nbuf)
                   (casting/numeric-byte-width
                    (dtype-proto/elemwise-datatype nbuf)))]
    (NativeBinaryBuffer. nbuf (.address nbuf) n-bytes {})))
