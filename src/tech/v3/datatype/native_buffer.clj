(ns tech.v3.datatype.native-buffer
  "Support for malloc/free and generalized support for reading/writing typed data
  to a long integer address of memory."
  (:require [tech.v3.resource :as resource]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy :as copy]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.graal-native :as graal-native]
            [tech.v3.parallel.for :as parallel-for]
            [clojure.tools.logging :as log]
            [clj-commons.primitive-math :as pmath]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf])
  (:import [tech.v3.datatype UnsafeUtil]
           [sun.misc Unsafe]
           [tech.v3.datatype Buffer BufferCollection BinaryBuffer
            LongBuffer DoubleBuffer]
           [clojure.lang RT IObj Counted Indexed IFn
            IFn$LL IFn$LD IFn$LO IFn$LLO IFn$LDO IFn$LOO IFn$OLO
            IFn$LLL IFn$LLD IFn$LLO IFn$LLLO IFn$LLDO IFn$LLOO]
           [ham_fisted Casts Transformables IMutList ChunkedList Reductions
            ArrayHelpers]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn unsafe
  "Get access to an instance of sun.misc.Unsafe."
  ^Unsafe []
  UnsafeUtil/unsafe)


(defmacro ^:private adj-address
  [datatype address]
  (case datatype
    :int8 `(pmath/+ ~address ~'idx)
    :uint8 `(pmath/+ ~address ~'idx)
    :boolean `(pmath/+ ~address ~'idx)
    :int16 `(pmath/+ ~address (pmath/* ~'idx 2))
    :uint16 `(pmath/+ ~address (pmath/* ~'idx 2))
    :char `(pmath/+ ~address (pmath/* ~'idx 2))
    :int32 `(pmath/+ ~address (pmath/* ~'idx 4))
    :uint32 `(pmath/+ ~address (pmath/* ~'idx 4))
    :float32 `(pmath/+ ~address (pmath/* ~'idx 4))
    :int64 `(pmath/+ ~address (pmath/* ~'idx 8))
    :uint64 `(pmath/+ ~address (pmath/* ~'idx 8))
    :float64 `(pmath/+ ~address (pmath/* ~'idx 8))))


(defn raw-read
  [datatype swap?]
  (case datatype
    :int8 '.getByte
    :uint8 '.getByte
    :boolean '.getByte
    :int16 '.getShort
    :uint16 '.getShort
    :char '.getShort
    :int32 '.getInt
    :uint32 '.getInt
    :float32 (if swap? '.getInt '.getFloat)
    :int64 '.getLong
    :uint64 '.getLong
    :float64 (if swap? '.getLong '.getDouble)))


(defn raw-write
  [datatype swap?]
  (case datatype
    :int8 '.putByte
    :uint8 '.putByte
    :boolean '.putByte
    :int16 '.putShort
    :uint16 '.putShort
    :char '.putShort
    :int32 '.putInt
    :uint32 '.putInt
    :float32 (if swap? '.putInt '.putFloat)
    :int64 '.putLong
    :uint64 '.putLong
    :float64 (if swap? '.putLong '.putDouble)))


(defmacro swap-value
  [value datatype swap?]
  (if (not swap?)
    `~value
    (case datatype
      :int8 `~value
      :uint8 `~value
      :boolean `~value
      :int16 `(Short/reverseBytes ~value)
      :uint16 `(Short/reverseBytes ~value)
      :char `(Short/reverseBytes ~value)
      :int32 `(Integer/reverseBytes ~value)
      :uint32 `(Integer/reverseBytes ~value)
      :float32 `(Integer/reverseBytes ~value)
      :int64 `(Long/reverseBytes ~value)
      :uint64 `(Long/reverseBytes ~value)
      :float64 `(Long/reverseBytes ~value))))


(defmacro data->jvm
  [value datatype swap?]
  (case datatype
    :int8 `(unchecked-long ~value)
    :uint8 `(unchecked-long (Byte/toUnsignedInt ~value))
    :boolean `(Casts/longCast ~value)
    :int16 `(unchecked-long ~value)
    :uint16 `(unchecked-long (Short/toUnsignedInt ~value))
    :char `(unchecked-char ~value)
    :int32 `~value
    :uint32 `(Integer/toUnsignedLong ~value)
    :float32 (if swap? `(Float/intBitsToFloat ~value) `~value)
    :int64 `~value
    :uint64 `~value
    :float64 (if swap? `(Double/longBitsToDouble ~value) `~value)))


(defmacro jvm->data
  [value datatype swap?]
  (case datatype
    :int8 `(unchecked-byte ~value)
    :uint8 `(unchecked-byte ~value)
    :boolean `(unchecked-byte (Casts/longCast ~value))
    :int16 `(unchecked-short ~value)
    :uint16 `(unchecked-short ~value)
    :char `(unchecked-short ~value)
    :int32 `(unchecked-int ~value)
    :uint32 `(unchecked-int ~value)
    :float32 (if swap? `(Float/floatToIntBits ~value) `(unchecked-float ~value))
    :int64 `~value
    :uint64 `~value
    :float64 (if swap? `(Double/doubleToLongBits ~value) `~value)))


(defmacro ^:private native-buffer-accessors
  [datatype swap?]
  `{:get-fn ~(cond
               (casting/integer-type? datatype)
               `(fn ~(with-meta [(with-meta 'address {:tag 'long})
                                 (with-meta 'idx {:tag 'long})]
                       {:tag 'long})
                  (-> (~(raw-read datatype swap?) (unsafe)
                       (adj-address ~datatype ~'address))
                      (swap-value ~datatype ~swap?)
                      (data->jvm ~datatype ~swap?)))
               (casting/float-type? datatype)
               `(fn ~(with-meta [(with-meta 'address {:tag 'long})
                                 (with-meta 'idx {:tag 'long})]
                       {:tag 'double})
                  (-> (~(raw-read datatype swap?) (unsafe)
                       (adj-address ~datatype ~'address))
                      (swap-value ~datatype ~swap?)
                      (data->jvm ~datatype ~swap?)))
               (or (identical? datatype :boolean)
                   (identical? datatype :char))
               `(fn [~(with-meta 'address {:tag 'long})
                     ~(with-meta 'idx {:tag 'long})]
                  (-> (~(raw-read datatype swap?) (unsafe)
                       (adj-address ~datatype ~'address))
                      (data->jvm ~datatype ~swap?)))
               :else
               (throw (Exception. (str "Unrecognized datatype: " ~datatype))))
    :set-fn ~(cond
               (casting/integer-type? datatype)
               `(fn [~(with-meta 'address {:tag 'long})
                     ~(with-meta 'idx {:tag 'long})
                     ~(with-meta 'arg {:tag 'long})]
                  (~(raw-write datatype swap?) (unsafe)
                   (adj-address ~datatype ~'address)
                   (-> (jvm->data ~'arg ~datatype ~swap?)
                       (swap-value ~datatype ~swap?))))
               (casting/float-type? datatype)
               `(fn [~(with-meta 'address {:tag 'long})
                     ~(with-meta 'idx {:tag 'long})
                     ~(with-meta 'arg {:tag 'double})]
                  (~(raw-write datatype swap?) (unsafe)
                   (adj-address ~datatype ~'address)
                   (-> (jvm->data ~'arg ~datatype ~swap?)
                       (swap-value ~datatype ~swap?))))
               (or (identical? datatype :boolean)
                   (identical? datatype :char))
               `(fn [~(with-meta 'address {:tag 'long})
                     ~(with-meta 'idx {:tag 'long})
                     ~'arg]
                  (~(raw-write datatype swap?) (unsafe)
                   (adj-address ~datatype ~'address)
                   (-> (jvm->data ~'arg ~datatype ~swap?)
                       (swap-value ~datatype ~swap?))))
               :else
               (throw (Exception. (str "Unrecognized datatype: " ~datatype))))})


(def accessor-maps
  (->> (for [dtype [:boolean :int8 :uint8 :int16 :uint16 :char :int32 :uint32 :int64 :uint64
                    :float32 :float64]
             swap? [true false]]
         [[dtype swap?]
          (if swap?
            (case dtype
              :boolean (native-buffer-accessors :boolean true)
              :int8 (native-buffer-accessors :int8 true)
              :uint8 (native-buffer-accessors :uint8 true)
              :int16 (native-buffer-accessors :int16 true)
              :uint16 (native-buffer-accessors :uint16 true)
              :char (native-buffer-accessors :char true)
              :int32 (native-buffer-accessors :int32 true)
              :uint32 (native-buffer-accessors :uint32 true)
              :int64 (native-buffer-accessors :int64 true)
              :uint64 (native-buffer-accessors :uint64 true)
              :float32 (native-buffer-accessors :float32 true)
              :float64 (native-buffer-accessors :float64 true))
            (case dtype
              :boolean (native-buffer-accessors :boolean false)
              :int8 (native-buffer-accessors :int8 false)
              :uint8 (native-buffer-accessors :uint8 false)
              :int16 (native-buffer-accessors :int16 false)
              :uint16 (native-buffer-accessors :uint16 false)
              :char (native-buffer-accessors :char false)
              :int32 (native-buffer-accessors :int32 false)
              :uint32 (native-buffer-accessors :uint32 false)
              :int64 (native-buffer-accessors :int64 false)
              :uint64 (native-buffer-accessors :uint64 false)
              :float32 (native-buffer-accessors :float32 false)
              :float64 (native-buffer-accessors :float64 false)))])
       (into {})))


(deftype ^:private PackedNativeBuf [datatype
                                    buffer
                                    ^long address
                                    ^long n-elems
                                    ^IFn$LLL get-fn
                                    ^IFn$LLLO set-fn
                                    pack-fn
                                    unpack-fn]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] buffer)
  dtype-proto/PEndianness
  (endianness [item] (dtype-proto/endianness buffer))
  dtype-proto/PDatatype
  (datatype [rdr] (dtype-proto/datatype buffer))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype] item)
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [item] true)
  (->binary-buffer [item]
    (dtype-proto/->binary-buffer buffer))
  dtype-proto/PSetConstant
  (set-constant! [this sidx len v] (dtype-proto/set-constant! buffer sidx len v))
  dtype-proto/PMemcpyInfo
  (memcpy-info [this] (dtype-proto/memcpy-info buffer))
  Object
  (toString [this] (.toString ^Object buffer))
  (equals [this o] (.equiv this o))
  (hashCode [this] (.hasheq this))
  Buffer
  (meta [rdr] (meta buffer))
  (withMeta [rdr m] (dtype-proto/->buffer (with-meta buffer m)))
  (elemwiseDatatype [rdr] datatype)
  (lsize [rdr] n-elems)
  (allowsRead [rdr] true)
  (allowsWrite [rdr] true)
  (subBuffer [rdr sidx eidx]
    (if (and (== sidx 0) (== (- eidx sidx) n-elems))
      rdr
      (-> (dtype-proto/sub-buffer buffer sidx (- eidx sidx))
          (dtype-proto/->buffer))))
  (readLong [rdr idx]
    (errors/check-idx idx n-elems)
    (.invokePrim get-fn address idx))
  (writeLong [rdr idx val]
    (errors/check-idx idx n-elems)
    (.invokePrim set-fn address idx val))
  (readDouble [rdr idx]
    (double (.readLong rdr idx)))
  (writeDouble [rdr idx val]
    (.writeLong rdr idx (Casts/longCast val)))
  (readObject [rdr idx]
    (errors/check-idx idx n-elems)
    (unpack-fn (.invokePrim get-fn address idx)))
  (writeObject [rdr idx val]
    (errors/check-idx idx n-elems)
    (.invokePrim set-fn address idx (long (pack-fn val))))
  (fillRange [this sidx eidx v] (dtype-proto/set-constant! buffer sidx (- eidx sidx) v))
  (reduce [rdr rfn acc]
    (let [addr address]
      (if (instance? IFn$OLO rdr)
        (loop [idx 0
               acc acc]
          (if (and (< idx n-elems) (not (reduced? acc)))
            (recur (unchecked-inc idx) (.invokePrim ^IFn$OLO rfn acc
                                                    (.invokePrim get-fn addr idx)))
            (Reductions/unreduce acc)))
        (loop [idx 0
               acc acc]
          (if (and (< idx n-elems) (not (reduced? acc)))
            (recur (unchecked-inc idx) (rfn acc (unpack-fn (.invokePrim get-fn addr idx))))
            (Reductions/unreduce acc)))))))


(dtype-pp/implement-tostring-print PackedNativeBuf)


(deftype ^:private LongNativeBuf [datatype
                                  buffer
                                  ^long address
                                  ^long n-elems
                                  ^IFn$LLL get-fn
                                  ^IFn$LLLO set-fn]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] buffer)
  dtype-proto/PEndianness
  (endianness [item] (dtype-proto/endianness buffer))
  dtype-proto/PDatatype
  (datatype [rdr] (dtype-proto/datatype buffer))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype] item)
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [item] true)
  (->binary-buffer [item]
    (dtype-proto/->binary-buffer buffer))
  dtype-proto/PSetConstant
  (set-constant! [this sidx len v] (dtype-proto/set-constant! buffer sidx len v))
  dtype-proto/PMemcpyInfo
  (memcpy-info [this] (dtype-proto/memcpy-info buffer))
  Object
  (toString [this] (.toString ^Object buffer))
  (equals [this o] (.equiv this o))
  (hashCode [this] (.hasheq this))
  LongBuffer
  (meta [rdr] (meta buffer))
  (withMeta [rdr m] (dtype-proto/->buffer (with-meta buffer m)))
  (elemwiseDatatype [rdr] datatype)
  (lsize [rdr] n-elems)
  (allowsRead [rdr] true)
  (allowsWrite [rdr] true)
  (subBuffer [rdr sidx eidx]
    (if (and (== sidx 0) (== (- eidx sidx) n-elems))
      rdr
      (-> (dtype-proto/sub-buffer buffer sidx (- eidx sidx))
          (dtype-proto/->buffer))))
  (readLong [rdr idx]
    (errors/check-idx idx n-elems)
    (.invokePrim get-fn address idx))
  (writeLong [rdr idx val]
    (errors/check-idx idx n-elems)
    (.invokePrim set-fn address idx val))
  (accumPlusLong [this idx val]
    (errors/check-idx idx n-elems)
    (.invokePrim set-fn address idx (+ val (.invokePrim get-fn address idx))))
  (fillRange [rdr sidx v]
    (ChunkedList/checkIndexRange 0 n-elems sidx (+ sidx (long (dtype-proto/ecount v))))
    (let [addr address]
      (reduce (hamf-rf/indexed-long-accum
               acc idx v (.invokePrim set-fn addr (+ idx sidx) v))
              nil
              v)))
  (fillRange [this sidx eidx v] (dtype-proto/set-constant! buffer sidx (- eidx sidx) v))
  (reduce [rdr rfn acc]
    (let [rfn (Transformables/toLongReductionFn rfn)
          addr address]
      (loop [idx 0
             acc acc]
        (if (and (< idx n-elems) (not (reduced? acc)))
          (recur (unchecked-inc idx) (.invokePrim rfn acc (.invokePrim get-fn addr idx)))
          (Reductions/unreduce acc))))))


(dtype-pp/implement-tostring-print LongNativeBuf)


(deftype ^:private DoubleNativeBuf [datatype
                                    buffer
                                    ^long address
                                    ^long n-elems
                                    ^IFn$LLD get-fn
                                    ^IFn$LLDO set-fn]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] buffer)
  dtype-proto/PEndianness
  (endianness [item] (dtype-proto/endianness buffer))
  dtype-proto/PDatatype
  (datatype [rdr] (dtype-proto/datatype buffer))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype] item)
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [item] true)
  (->binary-buffer [item]
    (dtype-proto/->binary-buffer buffer))
  dtype-proto/PSetConstant
  (set-constant! [this sidx len v] (dtype-proto/set-constant! buffer sidx len v))
  dtype-proto/PMemcpyInfo
  (memcpy-info [this] (dtype-proto/memcpy-info buffer))
  Object
  (toString [this] (.toString ^Object buffer))
  (equals [this o] (.equiv this o))
  (hashCode [this] (.hasheq this))
  DoubleBuffer
  (meta [rdr] (meta buffer))
  (withMeta [rdr m] (dtype-proto/->buffer (with-meta buffer m)))
  (elemwiseDatatype [rdr] datatype)
  (lsize [rdr] n-elems)
  (allowsRead [rdr] true)
  (allowsWrite [rdr] true)
  (subBuffer [rdr sidx eidx]
    (if (and (== sidx) (== (- eidx sidx) n-elems))
      rdr
      (-> (dtype-proto/sub-buffer buffer sidx (- eidx sidx))
          (dtype-proto/->buffer))))
  (readDouble [rdr idx]
    (errors/check-idx idx n-elems)
    (.invokePrim get-fn address idx))
  (writeDouble [rdr idx val]
    (errors/check-idx idx n-elems)
    (.invokePrim set-fn address idx val))
  (accumPlusDouble [this idx val]
    (errors/check-idx idx n-elems)
    (.invokePrim set-fn address idx (+ val (.invokePrim get-fn address idx))))
  (fillRange [this sidx eidx v] (dtype-proto/set-constant! buffer sidx (- eidx sidx) v))
  (fillRange [rdr sidx v]
    (ChunkedList/checkIndexRange 0 n-elems sidx (+ sidx (Casts/longCast
                                                         (dtype-proto/ecount v))))
    (let [addr address]
      (reduce (hamf-rf/indexed-double-accum
               acc idx v (.invokePrim set-fn addr (+ idx sidx) v))
              nil
              v)))
  (reduce [rdr rfn acc]
    (let [rfn (Transformables/toDoubleReductionFn rfn)
          addr address]
      (loop [idx 0 acc acc]
        (if (and (< idx n-elems) (not (reduced? acc)))
          (recur (unchecked-inc idx) (.invokePrim rfn acc (.invokePrim get-fn addr idx)))
          (Reductions/unreduce acc))))))


(dtype-pp/implement-tostring-print DoubleNativeBuf)


(deftype ObjectNativeBuf [datatype
                          buffer
                          ^long address
                          ^long n-elems
                          ^IFn$LLO get-fn
                          ^IFn$LLOO set-fn]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this] buffer)
  dtype-proto/PEndianness
  (endianness [item] (dtype-proto/endianness buffer))
  dtype-proto/PDatatype
  (datatype [rdr] (dtype-proto/datatype buffer))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype] item)
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [item] true)
  (->binary-buffer [item]
    (dtype-proto/->binary-buffer buffer))
  dtype-proto/PMemcpyInfo
  (memcpy-info [this] (dtype-proto/memcpy-info buffer))
  Object
  (toString [this] (.toString ^Object buffer))
  (equals [this o] (.equiv this o))
  (hashCode [this] (.hasheq this))
  Buffer
  (meta [rdr] (meta buffer))
  (withMeta [rdr m] (dtype-proto/->buffer (with-meta buffer m)))
  (elemwiseDatatype [rdr] datatype)
  (lsize [rdr] n-elems)
  (allowsRead [rdr] true)
  (allowsWrite [rdr] true)
  (subBuffer [rdr sidx eidx]
    (if (and (== sidx 0) ((- eidx sidx) == n-elems))
      rdr
      (-> (dtype-proto/sub-buffer buffer sidx (- eidx sidx))
          (dtype-proto/->buffer))))
  (readObject [rdr idx]
    (errors/check-idx idx n-elems)
    (.invokePrim get-fn address idx))
  (writeObject [rdr idx val]
    (errors/check-idx idx n-elems)
    (.invokePrim set-fn address idx val))
  (fillRange [this sidx eidx v] (dtype-proto/set-constant! buffer sidx (- eidx sidx) v))
  (reduce [rdr rfn acc]
    (let [addr address]
      (loop [idx 0 acc acc]
        (if (and (< idx n-elems) (not (reduced? acc)))
          (recur (unchecked-inc idx) (rfn acc (.invokePrim get-fn addr idx)))
          (Reductions/unreduce acc))))))


(dtype-pp/implement-tostring-print ObjectNativeBuf)



(defn- native-buffer->buffer-impl
  [datatype advertised-datatype buffer address n-elems accessor-map]
  (let [[pack-fn unpack-fn] (packing/packing-pair advertised-datatype)
        n-elems (long n-elems)
        {:keys [get-fn set-fn]} accessor-map
        ^NativeBuffer buffer buffer]
    (cond
      (casting/integer-type? datatype)
      (if pack-fn
        (PackedNativeBuf. advertised-datatype buffer address n-elems get-fn set-fn
                          pack-fn unpack-fn)
        (LongNativeBuf. advertised-datatype buffer address n-elems get-fn set-fn))
      (casting/float-type? datatype)
      (DoubleNativeBuf. advertised-datatype buffer address n-elems get-fn set-fn)
      :else
      (ObjectNativeBuf. advertised-datatype buffer address n-elems get-fn set-fn))))


(declare native-buffer->buffer native-buffer->map construct-binary-buffer)

(defmacro ^:private buffer!
  []
  `(do
     (when-not ~'cached-io
       (set! ~'cached-io (native-buffer->buffer ~'this)))
     ~'cached-io))

;;Size is in elements, not in bytes
(deftype NativeBuffer [^long address ^long n-elems datatype endianness
                       resource-type metadata
                       ^:unsynchronized-mutable ^Buffer cached-io
                       parent]
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [_this] true)
  (->native-buffer [this] this)
  dtype-proto/PEndianness
  (endianness [_item] endianness)
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [_this] datatype)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item _new-dtype]
    (or cached-io (dtype-proto/->reader item)))
  dtype-proto/PDatatype
  (datatype [_this] :native-buffer)
  dtype-proto/PECount
  (ecount [_this] n-elems)
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
    ;;Native buffers clone to the jvm heap.  This an assumption built into many parts
    ;;of the system.
    (dtype-proto/make-container :jvm-heap datatype nil this))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [_this] true)
  (->buffer [this] (buffer!))
  dtype-proto/PToReader
  (convertible-to-reader? [_this] true)
  (->reader [this] (buffer!))
  dtype-proto/PToWriter
  (convertible-to-writer? [_this] true)
  (->writer [this] (buffer!))
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [_buf] true)
  (->binary-buffer [buf] (construct-binary-buffer buf))
  dtype-proto/PMemcpyInfo
  (memcpy-info [this] [nil address])
  IMutList
  (meta [_item] metadata)
  (withMeta [item metadata]
    (NativeBuffer. address n-elems datatype endianness resource-type
                   metadata
                   cached-io
                   item))
  (size [_item] (int n-elems))
  (subList [item sidx eidx]
    (.sub-buffer item sidx (- eidx sidx)))
  (get [this idx] (.readObject (buffer!) idx))
  (getLong [this idx] (.readLong (buffer!) idx))
  (getDouble [this idx] (.readDouble (buffer!) idx))
  (set [this idx v]
    (let [b (buffer!)
          vv (.readObject b idx)]
      (.writeObject b idx v)
      vv))
  (setLong [this idx v] (.writeLong (buffer!) idx v))
  (setDouble [this idx v] (.writeDouble (buffer!) idx v))
  (nth [this idx] (.nth (buffer!) idx))
  (nth [this idx def-val] (.nth (buffer!) idx def-val))
  (invoke [this idx] (.invoke (buffer!) idx))
  (invoke [this idx value] (.invoke (buffer!) idx value))
  (fillRange [this idx v] (.fillRange (buffer!) idx v))
  (fillRange [this sidx eidx v] (.set-constant! this sidx (- eidx sidx) v))
  (reduce [this rfn acc]
    (if (identical? :int8 datatype)
      (let [us (unsafe)
            addr address
            ne (+ addr n-elems)
            rfn (Transformables/toLongReductionFn rfn)]
        (loop [idx addr
               acc acc]
          (if (and (< idx ne) (not (reduced? acc)))
            (recur (unchecked-inc idx) (.invokePrim rfn acc (.getByte us idx)))
            (if (reduced? acc) @acc acc))))
      (.reduce (buffer!) rfn acc)))
  (reduce [this rfn] (.reduce (buffer!) rfn))
  (parallelReduction [this ifn rfn mfn options]
    (.parallelReduction (buffer!) ifn rfn mfn options))
  Object
  (toString [this]
    (if-not (:record-print? metadata)
      (dtype-pp/buffer->string (buffer!) (format "native-buffer@0x%016X"
                                                 (.address this)))
      (Transformables/sequenceToString this)))
  (equals [this o] (.equiv this o))
  (hashCode [this] (.hasheq this)))


(dtype-pp/implement-tostring-print NativeBuffer)


(casting/add-object-datatype! :native-buffer NativeBuffer false)


(defn- native-buffer->buffer
  [^NativeBuffer this]
  (let [datatype (.elemwise-datatype this)
        address (.address this)
        n-elems (.n-elems this)
        swap? (not= (.endianness this) (dtype-proto/platform-endianness))
        un-aliased (casting/un-alias-datatype datatype)]
    (native-buffer->buffer-impl un-aliased datatype this (.-address this) n-elems
                                (get accessor-maps [un-aliased swap?]))))


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

(defn native-buffer->string
  "Convert a :int8 native buffer to a java string."
  ([^NativeBuffer nbuf ^long off ^long len]
   (when-not (identical? (.elemwise-datatype nbuf) :int8)
     (throw (RuntimeException. (str "Native buffer is not convertible to string - " (.datatype nbuf)))))
   (if (== 0 len)
     ""
     (let [final-len (+ off len)
           _ (when-not (<= final-len (.n-elems nbuf))
               (throw (RuntimeException. (str "Requested end point: " final-len
                                              " is past native buffer len" (.n-elems nbuf)))))
           addr (+ (.address nbuf) off)]
       (UnsafeUtil/addrToString addr (unchecked-int len)))))
  ([^NativeBuffer nbuf]
   (when-not (identical? (.elemwise-datatype nbuf) :int8)
     (throw (RuntimeException. "Native buffer is not convertible to string.")))
   (UnsafeUtil/addrToString (.address nbuf) (unchecked-int (.n-elems nbuf)))))


(defn native-buffer-byte-len
  "Get the length, in bytes, of a native buffer."
  ^long [^NativeBuffer nb]
  (let [original-size (.n-elems nb)
        nbdt (.elemwise-datatype nb)]
    (if (or (identical? nbdt :int8)
            (identical? nbdt :uint8))
      original-size
      (* original-size (casting/numeric-byte-width nbdt)))))


(defn set-native-datatype
  "Set the datatype of a native buffer.  n-elems will be recalculated."
  ^NativeBuffer [item datatype]
  (if (= datatype (dtype-proto/elemwise-datatype item))
    item
    (let [nb (as-native-buffer item)
          original-size (.n-elems nb)
          n-bytes (* original-size (casting/numeric-byte-width
                                    (dtype-proto/elemwise-datatype item)))
          new-byte-width (casting/numeric-byte-width
                          (casting/un-alias-datatype datatype))]
      (NativeBuffer. (.address nb) (quot n-bytes new-byte-width)
                     datatype (.endianness nb)
                     (.resource-type nb) (meta nb) nil item))))


(defn set-gc-obj
  ^NativeBuffer [^NativeBuffer nb gc-obj]
  (NativeBuffer. (.address nb) (.n-elems nb)
                 (.elemwise-datatype nb) (.endianness nb)
                 (.resource-type nb) (meta nb) nil gc-obj))


(defn set-parent
  "Return a new native-buffer that references the same data but with a different
  parent object."
  ^NativeBuffer [^NativeBuffer buf new-parent]
  (NativeBuffer. (.address buf) (.n-elems buf) (.elemwise-datatype buf)
                 (.endianness buf)
                 (.resource-type buf) (meta buf) nil new-parent))


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


(defn write-long
  "Ad-hoc write an integer at a given offset from a native buffer.  This method is not endian-aware."
  [^NativeBuffer native-buffer ^long offset ^long val]
  (assert (>= (- (native-buffer-byte-len native-buffer) offset 8) 0))
  (.putLong (unsafe) (+ (.address native-buffer) offset) val))


(defn read-int
  "Ad-hoc read an integer at a given offset from a native buffer.  This method is not endian-aware."
  (^long [^NativeBuffer native-buffer ^long offset]
   (assert (>= (- (native-buffer-byte-len native-buffer) offset 4) 0))
   (.getInt (unsafe) (+ (.address native-buffer) offset)))
  (^long [^NativeBuffer native-buffer]
   (assert (>= (- (native-buffer-byte-len native-buffer) 4) 0))
   (.getInt (unsafe) (.address native-buffer))))

(defn write-int
  "Ad-hoc write an integer at a given offset from a native buffer.  This method is not endian-aware."
  [^NativeBuffer native-buffer ^long offset ^long val]
  (assert (>= (- (native-buffer-byte-len native-buffer) offset 4) 0))
  (.putInt (unsafe) (+ (.address native-buffer) offset) (unchecked-int val)))


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
                                 :or {resource-type :auto}
                                 :as opts}]
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
   (let [address (long address)
         n-bytes (long n-bytes)]
     (errors/when-not-error
      (or (not (== 0 address))
          (== 0 n-bytes))
      "Attempt to wrap 0 as an address for a native buffer")
     (let [byte-width (casting/numeric-byte-width datatype)]
       (NativeBuffer. address (quot n-bytes byte-width)
                      datatype endianness #{:gc} nil nil gc-obj))))
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
  (clone [_this]
    (-> (dtype-proto/clone nbuf)
        (dtype-proto/->binary-buffer)))
  BinaryBuffer
  (lsize [_this] n-bytes)
  (allowsBinaryRead [_this] true)
  (readBinByte [_this byteOffset]
    (.getByte (unsafe) (check-bounds 1 byteOffset address n-bytes)))
  (readBinShort [_this byteOffset]
    (.getShort (unsafe) (check-bounds 2 byteOffset address n-bytes)))
  (readBinInt [_this byteOffset]
    (.getInt (unsafe) (check-bounds 4 byteOffset address n-bytes)))
  (readBinLong [_this byteOffset]
    (.getLong (unsafe) (check-bounds 8 byteOffset address n-bytes)))
  (readBinFloat [_this byteOffset]
    (.getFloat (unsafe) (check-bounds 4 byteOffset address n-bytes)))
  (readBinDouble [_this byteOffset]
    (.getDouble (unsafe) (check-bounds 8 byteOffset address n-bytes)))

  (allowsBinaryWrite [_this] true)
  (writeBinByte [_this byteOffset data]
    (.putByte (unsafe) (check-bounds 1 byteOffset address n-bytes) data))
  (writeBinShort [_this byteOffset data]
    (.putShort (unsafe) (check-bounds 2 byteOffset address n-bytes) data))
  (writeBinInt [_this byteOffset data]
    (.putInt (unsafe) (check-bounds 4 byteOffset address n-bytes) data))
  (writeBinLong [_this byteOffset data]
    (.putLong (unsafe) (check-bounds 8 byteOffset address n-bytes) data))
  (writeBinFloat [_this byteOffset data]
    (.putFloat (unsafe) (check-bounds 4 byteOffset address n-bytes) data))
  (writeBinDouble [_this byteOffset data]
    (.putDouble (unsafe) (check-bounds 8 byteOffset address n-bytes) data))
  IObj
  (meta [_this] metadata)
  (withMeta [_this newMeta]
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


(defn- unsafe-copy-memory
  [^NativeBuffer src-buf ^NativeBuffer dst-buf]
  (let [byte-width (casting/numeric-byte-width
                    (casting/un-alias-datatype (.-datatype src-buf)))]
    (.copyMemory (unsafe)
                 nil (.-address src-buf)
                 nil (.-address dst-buf)
                 (* (.-n-elems src-buf) byte-width))
    dst-buf))


(defn alloc-uninitialized
  "Allocate an uninitialized buffer.  See options for [[malloc]]."
  ([dtype ec] (alloc-uninitialized dtype ec nil))
  ([dtype ^long ec options]
   (-> (malloc (* ec (casting/numeric-byte-width dtype))
                    (assoc options :uninitialized? true))
       (set-native-datatype dtype))))


(defn alloc-zeros
  "Allocate a buffer of zeros.  See options for [[malloc]]."
  ([dtype ec] (alloc-zeros dtype ec nil))
  ([dtype ^long ec options]
   (-> (malloc (* ec (casting/numeric-byte-width dtype)) options)
       (set-native-datatype dtype))))


(defn ensure-native
  "If input is already a native buffer and has the same datatype as output,
  return input.  Else copy input into output and return output."
  ([input outbuf]
   (let [inb (when (dtype-proto/convertible-to-native-buffer? input)
               (dtype-proto/->native-buffer input))
         ec (long (dtype-proto/ecount outbuf))]
     (if (and (identical? (dtype-proto/elemwise-datatype input)
                          (dtype-proto/elemwise-datatype outbuf))
              (>= (long (dtype-proto/ecount input)) ec)
              inb)
       inb
       (do
         (if (dtype-proto/convertible-to-reader? input)
           (hamf/pgroups
            (dtype-proto/ecount outbuf)
            (fn [^long sidx ^long eidx]
              (if (and (== sidx 0) (== eidx ec))
                (.fillRange ^IMutList outbuf sidx input)
                (.fillRange ^IMutList outbuf sidx
                            (dtype-proto/sub-buffer input sidx (- eidx sidx))))))
           (reduce (hamf-rf/indexed-accum
                    acc idx v
                    (.set ^IMutList acc idx v)
                    acc)
                   outbuf
                   input))
         outbuf))))
  ([input]
   (if-let [inb (as-native-buffer input)]
     inb
     (copy/copy! input (alloc-uninitialized (dtype-proto/elemwise-datatype input)
                                            (Casts/longCast (dtype-proto/ecount input)))))))


(defn clone-native
  "Extremely fast clone assuming src is a native buffer. Exception otherwise.
  See options for [[malloc]]."
  ([data options]
   (let [^NativeBuffer nbuf (if (instance? NativeBuffer data)
                              data
                              (dtype-proto/->native-buffer data))
         ne (.-n-elems nbuf)
         dt (.-datatype nbuf)
         n-bytes (* (casting/numeric-byte-width dt) ne)
         rval (-> (malloc n-bytes (assoc options :uninitialized? true))
                  (set-native-datatype dt))]
     (unsafe-copy-memory nbuf rval)))
  ([data] (clone-native data nil)))


(defn as-native-buffer
  "If this item is convertible to a tech.v3.datatype.native_buffer.NativeBuffer
  return it.  Return nil otherwise."
  ^NativeBuffer [item]
  (if (instance? NativeBuffer item)
    item
    (when (dtype-proto/convertible-to-native-buffer? item)
      (dtype-proto/->native-buffer item))))


(comment
  (do
    (require '[criterium.core :as crit])
    (require '[tech.v3.datatype :as dt])

    (def src-str (apply str (repeat 10 "asldkejfalsdkjfalksdjfalksjdflaksdjflkajdsfasdfasdfasdfasdfasdfsdfhglkjsdflbkjnvliausdhfoiuahwefasdfkjasldkfjalksdjfalksdjflakjsdflkajsdflkjasdlfkjasdlkfjasdlkjfalksdhflaksdjbfv,mzcnxb vkjablskfjhalwiuehflakjsdbv,kjzdsbxvlkjasdhflkasdjhf")))

    (println (count src-str))

    (for [i (range 1 11)]
      (let [strlen (bit-shift-left 2 i)
            data (.substring src-str 0 strlen)
            dbuf (.getBytes data)
            src-nbuf (dt/make-container :native-heap :int8  dbuf)
            addr (.address src-nbuf)]
        (println "for-loop" strlen)
        (crit/quick-bench (UnsafeUtil/copyBytesLoop addr (byte-array strlen) strlen))
        (println "memcpy" strlen)
        (crit/quick-bench (UnsafeUtil/copyBytesMemcpy addr (byte-array strlen) strlen))))
    )
  )
