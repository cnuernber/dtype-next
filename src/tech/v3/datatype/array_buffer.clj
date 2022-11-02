(ns tech.v3.datatype.array-buffer
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.pprint :as dtype-pp]
            [ham-fisted.api :as hamf]
            [ham-fisted.iterator :as iterator]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [clojure.lang IObj Counted Indexed IFn IPersistentMap IReduceInit]
           [ham_fisted ArrayLists ArrayLists$ArrayOwner ArrayLists$ILongArrayList
            ArrayLists$ObjectArraySubList ArrayLists$ObjectArrayList
            ArrayLists$ByteArraySubList ArrayLists$ShortArraySubList
            ArrayLists$IntArraySubList ArrayLists$IntArrayList
            ArrayLists$LongArraySubList ArrayLists$LongArrayList
            ArrayLists$FloatArraySubList ArrayLists$CharArraySubList
            ArrayLists$DoubleArraySubList ArrayLists$DoubleArrayList
            ArrayLists$BooleanArraySubList ArrayLists$ILongArrayList
            ArraySection Transformables
            LongMutList Casts IMutList ImmutList ArrayImmutList MutList
            MutList$SubMutList ChunkedList TypedList]
           [ham_fisted.alists ByteArrayList ShortArrayList CharArrayList
            BooleanArrayList FloatArrayList]
           [tech.v3.datatype MutListBuffer]
           [tech.v3.datatype Buffer ArrayHelpers BufferCollection BinaryBuffer
            ByteConversions]
           [java.util Arrays RandomAccess List]
           [java.lang.reflect Array]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare array-sub-list)


(extend-type MutListBuffer
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype]
    (dtype-proto/elemwise-reader-cast (.-data this) new-dtype))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item]
    (dtype-proto/convertible-to-array-buffer? (.-data item)))
  (->array-buffer [item]
    (dtype-proto/->array-buffer (.-data item)))
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (.subBuffer item (long off) (+ (long off) (long len))))
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [item]
    (dtype-proto/convertible-to-binary-buffer? (.-data item)))
  (->binary-buffer [item]
    (dtype-proto/->binary-buffer (.-data item)))
  dtype-proto/PSetConstant
  (set-constant! [item off elem-count value]
    (.fillRange (.-data item) (int off) (+ (int off) (int elem-count)) value))
  dtype-proto/PClone
  (clone [this]
    (.cloneList this))
  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! (.-data raw-data) ary-target target-offset options))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] item)
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item] item)
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] item))


(defmethod print-method MutListBuffer
  [buf w]
  (.write ^java.io.Writer w (dtype-pp/buffer->string buf "buffer")))


(extend-type MutList
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [buf] :object)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype] (dtype-proto/->buffer this))
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (.subList item (int off) (+ (int off) (int len))))
  dtype-proto/PSetConstant
  (set-constant! [item off elem-count value]
    (.fillRange item (int off) (+ (int off) (int elem-count)) value))
  dtype-proto/PClone
  (clone [this]
    (.cloneList this))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] (MutListBuffer. item true :object))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item] (dtype-proto/->buffer item))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item)))


(extend-type MutList$SubMutList
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [buf] :object)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype] (dtype-proto/->buffer this))
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (.subList item (int off) (+ (int off) (int len))))
  dtype-proto/PSetConstant
  (set-constant! [item off elem-count value]
    (.fillRange item (int off) (+ (int off) (int elem-count)) value))
  dtype-proto/PClone
  (clone [this]
    (.cloneList this))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] (MutListBuffer. item true :object))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item] (dtype-proto/->buffer item))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item)))


(extend-type ImmutList
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [buf] :object)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype] (dtype-proto/->buffer this))
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (.subList item (int off) (+ (int off) (int len))))
  dtype-proto/PClone
  (clone [this] this)
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] (MutListBuffer. item false :object))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] false)
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item)))


(extend-type ArrayImmutList
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [buf] :object)
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (.subList ^List item off (+ (int off) (int len))))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype] (dtype-proto/->buffer this))
  dtype-proto/PClone
  (clone [this] this)
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] (MutListBuffer. item false :object))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] false)
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item)))


(defrecord ArrayBuffer [ary-data ^long offset ^long n-elems dtype]
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [buf] true)
  (->array-buffer [buf] buf)
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [buf] dtype)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype] (dtype-proto/->buffer this))
  dtype-proto/PECount
  (ecount [item] n-elems)
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] (MutListBuffer.
                    (array-sub-list dtype ary-data offset (+ offset n-elems) (meta item))
                    true dtype))
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (let [off (int off)
          len (int len)
          alen (Array/getLength ary-data)]
      (when (> (+ off len) alen)
        (throw (RuntimeException. (str "Out of range - extent: " (+ off len) " - alength: " alen))))
      (with-meta (ArrayBuffer. ary-data (+ offset off) len dtype) (meta item))))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item] (dtype-proto/->buffer item))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item))
  dtype-proto/PEndianness
  (endianness [item] :little-endian))


(casting/add-object-datatype! :array-buffer ArrayBuffer false)


(defmacro bind-array-list
  [alist-type ewise-dtype]
  `(extend-type ~alist-type
     dtype-proto/PDatatype
     (datatype [this#] :array-buffer)
     dtype-proto/PElemwiseDatatype
     (elemwise-datatype [~'this] (~ewise-dtype ~'this))
     dtype-proto/PElemwiseReaderCast
     (elemwise-reader-cast [this# new-dtype#] (dtype-proto/->buffer this#))
     dtype-proto/PSubBuffer
     (sub-buffer [this# offset# length#]
       (let [offset# (int offset#)
             length# (int length#)]
         (.subList this# offset# (+ offset# length#))))
     dtype-proto/PClone
     (clone [this#] (.cloneList this#))
     dtype-proto/PSetConstant
     (set-constant! [this# offset# elem-count# value#]
       (let [offset# (int offset#)
             elem-count# (int elem-count#)]
         (.fillRange this# offset# (+ offset# elem-count#) value#)))
     dtype-proto/PToBuffer
     (convertible-to-buffer? [this#] true)
     (->buffer [this#]
       (MutListBuffer. this# true (dtype-proto/elemwise-datatype this#)))
     dtype-proto/PToWriter
     (convertible-to-writer? [item#] true)
     (->writer [item#] (dtype-proto/->buffer item#))
     dtype-proto/PToReader
     (convertible-to-reader? [item#] true)
     (->reader [item#] (dtype-proto/->buffer item#))
     dtype-proto/PToArrayBuffer
     (convertible-to-array-buffer? [buf#] true)
     (->array-buffer [buf#]
       (let [section# (.getArraySection buf#)
             sidx# (.-sidx section#)
             eidx# (.-eidx section#)]
         (ArrayBuffer. (.-array section#) sidx# (.size section#)
                       (dtype-proto/elemwise-datatype buf#))))
     dtype-proto/PCopyRawData
     (copy-raw->item! [raw-data# ary-target# target-offset# options#]
       (dtype-proto/copy-raw->item! (dtype-proto/->array-buffer raw-data#)
                                    ary-target# target-offset# options#))
     dtype-proto/PEndianness
     (endianness [item#] :little-endian)))


(defn ^:private obj-cls->dtype
  [this]
  (casting/object-class->datatype (.containedType ^TypedList this)))


(bind-array-list ArrayLists$ObjectArraySubList obj-cls->dtype)
(bind-array-list ArrayLists$ObjectArrayList obj-cls->dtype)
(bind-array-list ArrayLists$BooleanArraySubList (constantly :boolean))
(bind-array-list BooleanArrayList (constantly :boolean))
(bind-array-list ArrayLists$ByteArraySubList (constantly :int8))
(bind-array-list ByteArrayList (constantly :int8))
(bind-array-list ArrayLists$ShortArraySubList (constantly :int16))
(bind-array-list ShortArrayList (constantly :int16))
(bind-array-list ArrayLists$CharArraySubList (constantly :char))
(bind-array-list CharArrayList (constantly :char))
(bind-array-list ArrayLists$IntArraySubList (constantly :int32))
(bind-array-list ArrayLists$IntArrayList (constantly :int32))
(bind-array-list ArrayLists$LongArraySubList (constantly :int64))
(bind-array-list ArrayLists$LongArrayList (constantly :int64))
(bind-array-list ArrayLists$FloatArraySubList (constantly :float32))
(bind-array-list FloatArrayList (constantly :float32))
(bind-array-list ArrayLists$DoubleArraySubList (constantly :float64))
(bind-array-list ArrayLists$DoubleArrayList (constantly :float64))


(defmacro ^:private host->long-uint8
  [v]
  `(Byte/toUnsignedInt ~v))

(defmacro ^:private long->host-uint8
  [v]
  `(unchecked-byte (casting/datatype->cast-fn :int64 :uint8 ~v)))

(defmacro ^:private object->host-uint8
  [v]
  `(long->host-uint8 (Casts/longCast ~v)))

(defmacro ^:private host->long-uint16
  [v]
  `(Short/toUnsignedInt ~v))

(defmacro ^:private long->host-uint16
  [v]
  `(unchecked-short (casting/datatype->cast-fn :int64 :uint16 ~v)))

(defmacro ^:private object->host-uint16
  [v]
  `(long->host-uint16 (Casts/longCast ~v)))


(defmacro ^:private host->long-uint32
  [v]
  `(Integer/toUnsignedLong ~v))

(defmacro ^:private long->host-uint32
  [v]
  `(unchecked-int (casting/datatype->cast-fn :int64 :uint32 ~v)))

(defmacro ^:private object->host-uint32
  [v]
  `(long->host-uint32 (Casts/longCast ~v)))


(defmacro ^:private host->long-uint64
  [v]
  `~v)

(defmacro ^:private long->host-uint64
  [v]
  `(unchecked-long (casting/datatype->cast-fn :int64 :uint64 ~v)))

(defmacro ^:private object->host-uint64
  [v]
  `(long->host-uint64 (Casts/longCast ~v)))


(defmacro make-unsigned-sub-list
  [list-name ary-type host->long long->host object->host]
  `(deftype ~list-name
       [~(with-meta 'data {:tag ary-type})
        ~(with-meta 'sidx {:tag 'long})
        ~(with-meta 'n-elems {:tag 'long})
        ~(with-meta 'm {:tag 'IPersistentMap})]
     Object
     (equals [this# other#] (.equiv this# other#))
     (hashCode [this#] (.hasheq this#))
     (toString [this#] (Transformables/sequenceToString this#))
     LongMutList
     (meta [this#] ~'m)
     (withMeta [this# newm#] (with-meta (.subList this# 0 ~'n-elems) newm#))
     (size [this#] ~'n-elems)
     (getLong [this# idx#]
       (~host->long (aget ~'data (+ (ArrayLists/checkIndex idx# ~'n-elems) ~'sidx))))
     (setLong [this# idx# v#]
       (ArrayHelpers/aset
        ~'data
        (+ (ArrayLists/checkIndex idx# ~'n-elems) ~'sidx)
        (~long->host v#)))
     (subList [this# ssidx# seidx#]
       (ChunkedList/sublistCheck ssidx# seidx# ~'n-elems)
       (~(symbol (str list-name ".")) ~'data (+ ~'sidx ssidx#) (- seidx# ssidx#) ~'m))
     ArrayLists$ArrayOwner
     (fill [this# ssidx# seidx# v#]
       (ArrayLists/checkIndexRange ~'n-elems ssidx# seidx#)
       (Arrays/fill ~'data (+ ~'sidx ssidx#) (+ ~'sidx seidx#) (~object->host v#)))
     (copyOfRange [this# ssidx# seidx#]
       (ArrayLists/checkIndexRange ~'n-elems ssidx# seidx#)
       (Arrays/copyOfRange ~'data (+ ~'sidx ssidx#) (+ ~'sidx seidx#)))
     (copyOf [this# len#]
       (Arrays/copyOfRange ~'data ~'sidx (+ ~'sidx len#)))
     (getArraySection [this#]
       (ArraySection. ~'data ~'sidx (+ ~'sidx ~'n-elems)))))


(make-unsigned-sub-list UByteArraySubList bytes
                        host->long-uint8
                        long->host-uint8
                        object->host-uint8)
(bind-array-list UByteArraySubList (constantly :uint8))

(make-unsigned-sub-list UShortArraySubList shorts
                        host->long-uint16
                        long->host-uint16
                        object->host-uint16)
(bind-array-list UShortArraySubList (constantly :uint16))

(make-unsigned-sub-list UIntArraySubList ints
                        host->long-uint32
                        long->host-uint32
                        object->host-uint32)
(bind-array-list UIntArraySubList (constantly :uint32))

(make-unsigned-sub-list ULongArraySubList longs
                        host->long-uint64
                        long->host-uint64
                        object->host-uint64)
(bind-array-list ULongArraySubList (constantly :uint64))


(definterface IGrowableList
  (^Object ensureCapacity [^long newlen]))


(defmacro make-unsigned-list
  [lname ary-tag host->long long->host object->host sublname]
  `(deftype ~lname [~(with-meta 'data {:unsynchronized-mutable true
                                       :tag ary-tag})
                    ~(with-meta 'n-elems {:unsynchronized-mutable true
                                          :tag 'long})
                    ~(with-meta 'm {:tag 'IPersistentMap})]
     Object
     (hashCode [this#] (.hasheq this#))
     (equals [this# other#] (.equiv this# other#))
     (toString [this#] (Transformables/sequenceToString this#))
     IGrowableList
     (ensureCapacity [this# newlen#]
       (when (> newlen# (alength ~'data))
         (set! ~'data (.copyOf this# (ArrayLists/newArrayLen newlen#))))
       ~'data)
     LongMutList
     (meta [this#] ~'m)
     (withMeta [this# newm#] (with-meta (.subList this# 0 ~'n-elems) newm#))
     (size [this#] (unchecked-int ~'n-elems))
     (getLong [this# idx#] (~host->long (aget ~'data (ArrayLists/checkIndex idx# ~'n-elems))))
     (setLong [this# idx# v#] (ArrayHelpers/aset ~'data
                                                 (ArrayLists/checkIndex idx# ~'n-elems)
                                                 (~long->host v#)))
     (subList [this# sidx# eidx#]
       (ChunkedList/sublistCheck sidx# eidx# ~'n-elems)
       (~(symbol (str sublname ".")) ~'data sidx# (- eidx# sidx#) ~'m))

     (addLong [this# v#]
       (let [curlen# ~'n-elems
             newlen# (unchecked-inc ~'n-elems)
             ~(with-meta 'b {:tag ary-tag}) (.ensureCapacity this# newlen#)]
         (ArrayHelpers/aset ~'b curlen# (~long->host v#))
         (set! ~'n-elems newlen#)))
     (add [this# idx# obj#]
       (ArrayLists/checkIndex idx# ~'n-elems)
       (if (== idx# ~'n-elems)
         (.add this# obj#)
         (let [bval# (~object->host obj#)
               curlen# ~'n-elems
               newlen# (unchecked-inc curlen#)
               ~(with-meta 'd {:tag ary-tag}) (.ensureCapacity this# newlen#)]
           (System/arraycopy ~'d idx# ~'d (unchecked-inc idx#) (- curlen# idx#))
           (ArrayHelpers/aset ~'d idx# (~object->host bval#))
           (set! ~'n-elems newlen#))))
     (addAllReducible [this# c#]
       (let [sz# (.size this#)]
         (if (instance? RandomAccess c#)
           (do
             (let [~(with-meta 'c {:tag 'List}) c#
                   curlen# ~'n-elems
                   newlen# (+ curlen# (.size ~'c))]
               (.ensureCapacity this# newlen#)
               (set! ~'n-elems newlen#)
               (.fillRange this# curlen# ~'c)))
           (Transformables/longReduce (fn [^IMutList lhs# ^long rhs#]
                                        (.addLong lhs# rhs#)
                                        lhs#)
                                      this# c#))
         (not (== sz# ~'n-elems))))
     (removeRange [this# sidx# eidx#]
       (ArrayLists/checkIndexRange ~'n-elems sidx# eidx#)
       (System/arraycopy ~'data sidx# ~'data eidx# (- ~'n-elems eidx#))
       (set! ~'n-elems (- ~'n-elems (- eidx# sidx#))))
     ArrayLists$ArrayOwner
     (fill [this# sidx# eidx# v#]
       (ArrayLists/checkIndexRange ~'n-elems sidx# eidx#)
       (Arrays/fill ~'data sidx# eidx# (~object->host v#)))
     (copyOfRange [this# sidx# eidx#]
       (Arrays/copyOfRange ~'data sidx# eidx#))
     (copyOf [this# len#]
       (Arrays/copyOf ~'data len#))
     (getArraySection [this#]
       (ArraySection. ~'data 0 ~'n-elems))))


(make-unsigned-list UByteArrayList bytes host->long-uint8 long->host-uint8
                    object->host-uint8 UByteArraySubList)
(bind-array-list UByteArrayList (constantly :uint8))

(make-unsigned-list UShortArrayList shorts host->long-uint16 long->host-uint16
                    object->host-uint16 UShortArraySubList)
(bind-array-list UShortArrayList (constantly :uint16))

(make-unsigned-list UIntArrayList ints host->long-uint32 long->host-uint32
                    object->host-uint32 UIntArraySubList)
(bind-array-list UIntArrayList (constantly :uint32))

(make-unsigned-list ULongArrayList longs host->long-uint64 long->host-uint64
                    object->host-uint64 ULongArraySubList)
(bind-array-list ULongArrayList (constantly :uint64))


(defn- host-array
  [dtype ^long n-elems]
  (case dtype
    :boolean (clojure.core/boolean-array n-elems)
    :int8 (clojure.core/byte-array n-elems)
    :int16 (clojure.core/short-array n-elems)
    :char (clojure.core/char-array n-elems)
    :int32 (clojure.core/int-array n-elems)
    :int64 (clojure.core/long-array n-elems)
    :float32 (clojure.core/float-array n-elems)
    :float64 (clojure.core/double-array n-elems)
    (clojure.core/make-array (or (casting/datatype->object-class dtype) Object) n-elems)))


(declare array-list)


(defn- ensure-array
  [item]
  (when (nil? item) (throw (Exception. "Nil data passed in")))
  (when-not (.isArray (.getClass ^Object item))
    (throw (Exception. "Data passed in is not an array")))
  item)


(defn- ensure-datatypes
  [ary-dtype buf-dtype]
  (when-not (or (identical? ary-dtype (casting/datatype->host-datatype buf-dtype)))
    (throw (Exception. (str "Array datatype " ary-dtype " and buffer datatype " buf-dtype
                            "are not compatible"))))
  buf-dtype)


(defn array-sub-list
  (^IMutList [dtype] (array-sub-list dtype 0))
  (^IMutList [dtype data]
   (let [data (if (number? data) data (hamf/->reducible data))
         n-elems (cond
                   (number? data)
                   (long data)
                   (instance? RandomAccess data)
                   (.size ^List data))]
     (if n-elems
       (let [src-data (host-array (casting/datatype->host-datatype dtype) n-elems)
             m (meta data)
             retval (case dtype
                        (case dtype
                          :uint8 (UByteArraySubList. src-data 0 n-elems m)
                          :uint16 (UShortArraySubList. src-data 0 n-elems m)
                          :uint32 (UIntArraySubList. src-data 0 n-elems m)
                          :uint64 (ULongArraySubList. src-data 0 n-elems m)
                          (ArrayLists/toList src-data)))]
         (when (instance? RandomAccess data)
           (.fillRange ^IMutList retval 0 data))
         retval)
       (let [alist (array-list dtype data)]
         (hamf/subvec alist 0)))))
  (^IMutList [dtype data sidx eidx m]
   (ensure-datatypes (dtype-proto/elemwise-datatype data) dtype)
   (let [ne (- (long eidx) (long sidx))]
     (case dtype
       :uint8 (UByteArraySubList. data sidx ne m)
       :uint16 (UShortArraySubList. data sidx ne m)
       :uint32 (UIntArraySubList. data sidx ne m)
       :uint64 (ULongArraySubList. data sidx ne m)
       (ArrayLists/toList data (long sidx) (long eidx) ^IPersistentMap m)))))


(defn array-list
  (^IMutList [dtype] (array-list dtype 4))
  (^IMutList [dtype data]
   (let [data (if (number? data) data (hamf/->reducible data))
         n-elems (cond (number? data)
                       (long data)
                       (instance? RandomAccess data)
                       (.size ^List data)
                       :else
                       4)
         src-data (host-array (casting/datatype->host-datatype dtype) n-elems)
         ^IMutList retval (case dtype
                            :boolean (BooleanArrayList. src-data 0 nil)
                            :int8 (ByteArrayList. src-data 0 nil)
                            :uint8 (UByteArrayList. src-data 0 nil)
                            :int16 (ShortArrayList. src-data 0 nil)
                            :uint16 (UShortArrayList. src-data 0 nil)
                            :char (CharArrayList. src-data 0 nil)
                            :int32 (ArrayLists$IntArrayList. src-data 0 nil)
                            :uint32 (UIntArrayList. src-data 0 nil)
                            :int64 (ArrayLists$LongArrayList. src-data 0 nil)
                            :uint64 (ULongArrayList. src-data 0 nil)
                            :float32 (FloatArrayList. src-data 0 nil)
                            :float64 (ArrayLists$DoubleArrayList. src-data 0 nil)
                            (ArrayLists$ObjectArrayList. src-data 0 nil))]
     (when-not (number? data)
       (.addAllReducible retval data))
     retval)))


(defn as-growable-list
  ^IMutList [data ^long ptr]
  (when-not (dtype-proto/convertible-to-array-buffer? data)
    (throw (RuntimeException. "Buffer not convertible to array buffer")))
  (let [^ArrayBuffer abuf (dtype-proto/->array-buffer data)]
    (when-not (== 0 (.-offset abuf))
      (throw (RuntimeException. "Only non-sub-buffer containers can become growable lists.")))
    (when-not (<= ptr (.-n-elems abuf))
      (throw (RuntimeException. "ptr out of range of buffer size")))
    (case (dtype-proto/elemwise-datatype abuf)
      :boolean (BooleanArrayList. (.-ary-data abuf) ptr (meta data))
      :int8 (ByteArrayList. (.-ary-data abuf) ptr (meta data))
      :uint8 (UByteArrayList. (.-ary-data abuf) ptr (meta data))
      :int16 (ShortArrayList. (.-ary-data abuf) ptr (meta data))
      :uint16 (UShortArrayList. (.-ary-data abuf) ptr (meta data))
      :char (CharArrayList. (.-ary-data abuf) ptr (meta data))
      :int32 (ArrayLists$IntArrayList. (.-ary-data abuf) ptr (meta data))
      :uint32 (UIntArrayList. (.-ary-data abuf) ptr (meta data))
      :int64 (ArrayLists$LongArrayList. (.-ary-data abuf) ptr (meta data))
      :uint64 (ULongArrayList. (.-ary-data abuf) ptr (meta data))
      :float32 (FloatArrayList. (.-ary-data abuf) ptr (meta data))
      :float64 (ArrayLists$DoubleArrayList. (.-ary-data abuf) ptr (meta data))
      (ArrayLists$ObjectArrayList. (.-ary-data abuf) ptr (meta data)))))



(defn array-buffer
  ([java-ary]
   (array-buffer java-ary (dtype-proto/elemwise-datatype java-ary)))
  ([java-ary buf-dtype]
   (let [ary-dtype (dtype-proto/elemwise-datatype java-ary)]
     (ArrayBuffer. (ensure-array java-ary) 0 (Array/getLength java-ary)
                   (ensure-datatypes ary-dtype buf-dtype)))))


(defn array-buffer->map
  "Convert an array buffer to a map of
  {:java-array :offset :length :datatype}"
  [^ArrayBuffer ary-buf]
  {:java-array (.ary-data ary-buf)
   :offset (.offset ary-buf)
   :length (.n-elems ary-buf)
   :datatype (.dtype ary-buf)
   :metadata (.meta ary-buf)})


(defn is-array-type?
  [item]
  (when item
    (.isArray (.getClass ^Object item))))


(def array-types
  (set (concat casting/host-numeric-types
               [:boolean :object :char])))


(defn- as-object ^Object [item] item)


(defmacro initial-implement-arrays
  []
  `(do
     ~@(->>
        array-types
        (map
         (fn [ary-type]
           (let [ary-dtype (keyword (str (name ary-type) "-array"))]
             `(do
                (casting/add-object-datatype! ~ary-dtype ~(typecast/datatype->array-cls ary-type)
                                              false)
                (extend-type ~(typecast/datatype->array-cls ary-type)
                  dtype-proto/PElemwiseDatatype
                  (elemwise-datatype [~'item]
                    ~(if (not= ary-type :object)
                       `~ary-type
                       `(-> (as-object ~'item)
                            (.getClass)
                            (.getComponentType)
                            (casting/object-class->datatype))))
                  dtype-proto/PElemwiseReaderCast
                  (elemwise-reader-cast [this# new-dtype#] (dtype-proto/->buffer this#))
                  dtype-proto/PDatatype
                  (datatype [item#] ~ary-dtype)
                  dtype-proto/PECount
                  (ecount [item#]
                    (alength
                     (typecast/datatype->array ~ary-type item#)))
                  dtype-proto/PEndianness
                  (endianness [item#] :little-endian)
                  dtype-proto/PToArrayBuffer
                  (convertible-to-array-buffer? [item#] true)
                  (->array-buffer [item#]
                    (ArrayBuffer. item# 0
                                  (alength (typecast/datatype->array ~ary-type
                                                                     item#))
                                  (dtype-proto/elemwise-datatype item#)))
                  dtype-proto/PSubBuffer
                  (sub-buffer [item# off# len#]
                    (.subList (hamf/->random-access item#) off# (+ (long off#) (long len#))))
                  dtype-proto/PCopyRawData
                  (copy-raw->item! [raw-data# ary-target# target-offset# options#]
                    (dtype-proto/copy-raw->item! (dtype-proto/->array-buffer raw-data#)
                                                 ary-target# target-offset# options#))
                  dtype-proto/PToBuffer
                  (convertible-to-buffer? [item#] true)
                  (->buffer [item#] (MutListBuffer. (hamf/->random-access item#) true
                                                    (dtype-proto/elemwise-datatype item#)))
                  dtype-proto/PToReader
                  (convertible-to-reader? [item#] true)
                  (->reader [item#] (dtype-proto/->buffer item#))
                  dtype-proto/PToWriter
                  (convertible-to-writer? [item#] true)
                  (->writer [item#] (dtype-proto/->buffer item#))))))))))


(initial-implement-arrays)


(deftype ByteArrayBinaryBufferLE [^bytes ary-data
                                  ^Buffer buffer
                                  ^long offset
                                  ^long n-elems]
  dtype-proto/PEndianness
  (endianness [_this] :little-endian)
  BinaryBuffer
  (lsize [_this] n-elems)
  (allowsBinaryRead [_this] true)
  (readBinByte [_this byteOffset] (aget ary-data (+ offset byteOffset)))
  (readBinShort [_this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/shortFromBytesLE (aget ary-data byteOffset)
                                        (aget ary-data (+ byteOffset 1)))))
  (readBinInt [_this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/intFromBytesLE (aget ary-data byteOffset)
                                      (aget ary-data (+ byteOffset 1))
                                      (aget ary-data (+ byteOffset 2))
                                      (aget ary-data (+ byteOffset 3)))))
  (readBinLong [_this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/longFromBytesLE (aget ary-data byteOffset)
                                       (aget ary-data (+ byteOffset 1))
                                       (aget ary-data (+ byteOffset 2))
                                       (aget ary-data (+ byteOffset 3))
                                       (aget ary-data (+ byteOffset 4))
                                       (aget ary-data (+ byteOffset 5))
                                       (aget ary-data (+ byteOffset 6))
                                       (aget ary-data (+ byteOffset 7)))))
  (readBinFloat [_this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/floatFromBytesLE (aget ary-data byteOffset)
                                        (aget ary-data (+ byteOffset 1))
                                        (aget ary-data (+ byteOffset 2))
                                        (aget ary-data (+ byteOffset 3)))))
  (readBinDouble [_this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/doubleFromBytesLE (aget ary-data byteOffset)
                                         (aget ary-data (+ byteOffset 1))
                                         (aget ary-data (+ byteOffset 2))
                                         (aget ary-data (+ byteOffset 3))
                                         (aget ary-data (+ byteOffset 4))
                                         (aget ary-data (+ byteOffset 5))
                                         (aget ary-data (+ byteOffset 6))
                                         (aget ary-data (+ byteOffset 7)))))

  (allowsBinaryWrite [_this] true)
  (writeBinByte [_this byteOffset data]
    (aset ary-data (+ byteOffset offset) data))
  (writeBinShort [_this byteOffset data]
    (ByteConversions/shortToWriterLE data buffer (+ byteOffset offset)))
  (writeBinInt [_this byteOffset data]
    (ByteConversions/intToWriterLE data buffer (+ byteOffset offset)))
  (writeBinLong [_this byteOffset data]
    (ByteConversions/longToWriterLE data buffer (+ byteOffset offset)))
  (writeBinFloat [_this byteOffset data]
    (ByteConversions/floatToWriterLE data buffer (+ byteOffset offset)))
  (writeBinDouble [_this byteOffset data]
    (ByteConversions/doubleToWriterLE data buffer (+ byteOffset offset)))
  dtype-proto/PClone
  (clone [_this]
    (-> (dtype-proto/sub-buffer ary-data offset n-elems)
        (dtype-proto/clone)
        (dtype-proto/->binary-buffer)))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [_this] true)
  (->array-buffer [_this]
    (-> (dtype-proto/->array-buffer ary-data)
        (dtype-proto/sub-buffer offset n-elems)))
  dtype-proto/PSubBuffer
  (sub-buffer [this off len]
    (let [off (long off)
          len (long len)]
      (ByteArrayBinaryBufferLE. ary-data
                                (-> (dtype-proto/->array-buffer this)
                                    (dtype-proto/sub-buffer off len)
                                    (dtype-proto/->buffer))
                                (+ offset off)
                                len))))


(extend-protocol dtype-proto/PToBinaryBuffer
  (Class/forName "[B")
  (convertible-to-binary-buffer? [ary] true)
  (->binary-buffer [ary]
    (ByteArrayBinaryBufferLE. ary
                              (dtype-proto/->buffer ary)
                              0 (alength ^bytes ary)))
  ArrayLists$ByteArraySubList
  (convertible-to-binary-buffer? [ary] true)
  (->binary-buffer [ary]
    (let [section (.getArraySection ary)]
      (ByteArrayBinaryBufferLE. (.-array section)
                                (dtype-proto/->buffer ary)
                                (.-sidx section) (.size section))))
  ByteArrayList
  (convertible-to-binary-buffer? [ary] true)
  (->binary-buffer [ary]
    (let [section (.getArraySection ary)]
      (ByteArrayBinaryBufferLE. (.-array section)
                                (dtype-proto/->buffer ary)
                                (.-sidx section) (.size section)))))
