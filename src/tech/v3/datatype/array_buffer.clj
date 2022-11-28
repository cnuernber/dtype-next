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
           [tech.v3.datatype MutListBuffer PackingMutListBuffer UByteSubBuffer]
           [tech.v3.datatype Buffer ArrayHelpers BufferCollection BinaryBuffer
            ByteConversions NumericConversions]
           [java.util Arrays RandomAccess List]
           [java.lang.reflect Array]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare array-sub-list set-datatype)


(extend-type MutListBuffer
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype]
    (dtype-proto/elemwise-reader-cast (.-data this) new-dtype))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item]
    (dtype-proto/convertible-to-array-buffer? (.-data item)))
  (->array-buffer [item]
    (dtype-proto/->array-buffer (.-data item)))
  (convertible-to-binary-buffer? [item]
    (dtype-proto/convertible-to-binary-buffer? (.-data item)))
  (->binary-buffer [item]
    (dtype-proto/->binary-buffer (.-data item)))
  dtype-proto/PCopyRawData
  (copy-raw->item! [raw-data ary-target target-offset options]
    (dtype-proto/copy-raw->item! (.-data raw-data) ary-target target-offset options)))


(defmethod print-method MutListBuffer
  [buf w]
  (.write ^java.io.Writer w (dtype-pp/buffer->string buf "buffer")))


(extend-type PackingMutListBuffer
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype] this)
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item]
    (dtype-proto/convertible-to-array-buffer? (.-data item)))
  (->array-buffer [item]
    (-> (dtype-proto/->array-buffer (.-data item))
        (set-datatype (.-elemwiseDatatype item)))))


(defmethod print-method PackingMutListBuffer
  [buf w]
  (.write ^java.io.Writer w (dtype-pp/buffer->string buf "buffer")))


(defn- ml-reader-cast
  [^IMutList l new-dtype]
  (dtype-proto/->buffer l))

(defn- ml-sub-buf
  [^IMutList l ^long off ^long len]
  (.subList l (int off) (int (+ off len))))

(defn- ml-set-constant!
  [^IMutList item ^long off ^long elem-count v]
  (.fillRange item (int off) (int (+ off elem-count)) v))

(defn- ml-clone
  [^IMutList item]
  (.cloneList item))

(defn- ml-to-buffer
  ^Buffer [^IMutList item]
  (let [item-dt (dtype-proto/elemwise-datatype item)]
    (if-let [[pack-fn unpack-fn] (packing/packing-pair item-dt)]
      (PackingMutListBuffer. item true item-dt pack-fn unpack-fn)
      (MutListBuffer. item true item-dt))))


(defn- extend-ml-type!
  [ml-type dtype allows-write]
  ;;ml-type had better extend IMutList
  (let [dtype-fn (if (keyword? dtype)
                   (constantly dtype)
                   dtype)]
    (extend ml-type
      dtype-proto/PElemwiseDatatype
      {:elemwise-datatype dtype-fn}
      dtype-proto/PElemwiseReaderCast
      {:elemwise-reader-cast ml-reader-cast}
      dtype-proto/PSubBuffer
      {:sub-buffer ml-sub-buf}
      dtype-proto/PSetConstant
      {:set-constant! ml-set-constant!}
      dtype-proto/PClone
      {:clone ml-clone}
      dtype-proto/PToBuffer
      {:convertible-to-buffer? (constantly true)
       :->buffer ml-to-buffer}
      dtype-proto/PToReader
      {:convertible-to-reader? (constantly true)
       :->reader ml-to-buffer}
      dtype-proto/PToWriter
      {:convertible-to-writer? (constantly allows-write)
       :->writer ml-to-buffer})))



;;Using manual extension to reduce the number of IFn's generated
;;and overall reduce compile times.
(extend-ml-type! MutList :object true)
(extend-ml-type! MutList$SubMutList :object true)
(extend-ml-type! ImmutList :object false)
(extend-ml-type! ArrayImmutList :object false)


;;Only returns implementations of Buffer
(defn- array-list->buffer
  [datalist dtype]
  (if (instance? Buffer datalist)
    datalist
    (if-let [[pack-fn unpack-fn] (packing/packing-pair dtype)]
      (PackingMutListBuffer. datalist true dtype pack-fn unpack-fn)
      (MutListBuffer. datalist true dtype))))


;;May return implementation of Buffer or just IMutList
(defn- array-list->packed-list
  [datalist dtype]
  (if (instance? Buffer datalist)
    datalist
    (if-let [[pack-fn unpack-fn] (packing/packing-pair dtype)]
      (PackingMutListBuffer. datalist true dtype pack-fn unpack-fn)
      datalist)))


(defmacro buffer!
  []
  `(do (when-not ~'buffer) (set! ~'buffer (dtype-proto/->buffer ~'this))
       ~'buffer))


(deftype ArrayBuffer [ary-data ^long offset ^long n-elems dtype
                      meta
                      ^{:unsynchronized-mutable true
                        :tag Buffer} buffer]
  Object
  (toString [this] (dtype-pp/buffer->string (dtype-proto/->buffer this) "array-buffer"))
  (equals [this o] (.equiv this o))
  (hashCode [this] (.hasheq this))
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
  (->buffer [item]
    (when-not buffer
      (set! buffer
            (-> (array-sub-list dtype ary-data offset (+ offset n-elems) meta)
                (array-list->buffer dtype))))
    buffer)
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (let [off (int off)
          len (int len)
          alen (Array/getLength ary-data)]
      (when (> (+ off len) alen)
        (throw (RuntimeException. (str "Out of range - extent: " (+ off len) " - alength: " alen))))
      (ArrayBuffer. ary-data (+ offset off) len dtype (meta item) buffer)))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item] (dtype-proto/->buffer item))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item))
  dtype-proto/PEndianness
  (endianness [item] :little-endian)
  IMutList
  (meta [this] meta)
  (withMeta [this newm] (ArrayBuffer. ary-data offset n-elems dtype newm buffer))
  (size [this] (unchecked-int n-elems))
  (get [this idx] (.readObject (buffer!) idx))
  (getLong [this idx] (.readLong (buffer!) idx))
  (getDouble [this idx] (.readDouble (buffer!) idx))
  (set [this idx v] (let [rv (.readObject (buffer!) idx)]
                      (.writeObject (buffer!) idx v)
                      rv))
  (setLong [this idx v] (.writeLong (buffer!) idx v))
  (setDouble [this idx v] (.writeDouble (buffer!) idx v))
  (cloneList [this] (.cloneList (buffer!)))
  (reduce [this rfn] (.reduce (buffer!) rfn))
  (reduce [this rfn init] (.reduce (buffer!) rfn init)))


(dtype-pp/implement-tostring-print ArrayBuffer)


(defn set-datatype
  "Set the array-buffer's datatype.  This way you can take an int32 array buffer
  and make it a packed-local-date, or vice versa.  If it isn't obvious, use this
  with some care."
  [^ArrayBuffer abuf dtype]
  (ArrayBuffer. (.-ary-data abuf) (.-offset abuf) (.-n-elems abuf) dtype nil nil))

(casting/add-object-datatype! :array-buffer ArrayBuffer false)

(defn- array-owner->array-buffer
  [^ArrayLists$ArrayOwner owner]
  (let [section (.getArraySection owner)
        sidx (.-sidx section)
        eidx (.-eidx section)]
    (ArrayBuffer. (.-array section) sidx (- eidx sidx)
                  (dtype-proto/elemwise-datatype owner)
                  nil nil)))

(defn array-buffer-convertible-copy-raw-data
  "Given an array buffer convertible object implement copy-raw->data"
  [raw-data ary-target ary-offset options]
  (dtype-proto/copy-raw->item! (dtype-proto/->array-buffer raw-data)
                               ary-target ary-offset options))

(defn- extend-array-owner!
  [alist-type]
  (extend alist-type
    dtype-proto/PToArrayBuffer
    {:convertible-to-array-buffer? (constantly true)
     :->array-buffer array-owner->array-buffer}
    dtype-proto/PCopyRawData
    {:copy-raw->item! array-buffer-convertible-copy-raw-data}
    dtype-proto/PEndianness
    {:endianness (constantly :little-endian)}))


(defn bind-array-list
  [alist-type ewise-dtype-fn]
  (extend-ml-type! alist-type ewise-dtype-fn true)
  (extend-array-owner! alist-type))


(defn ^:private obj-cls->dtype
  [this]
  (casting/object-class->datatype (.containedType ^TypedList this)))


(bind-array-list ArrayLists$ObjectArraySubList obj-cls->dtype)
(bind-array-list ArrayLists$ObjectArrayList obj-cls->dtype)
(bind-array-list ArrayLists$BooleanArraySubList :boolean)
(bind-array-list BooleanArrayList :boolean)
(bind-array-list ArrayLists$ByteArraySubList :int8)
(bind-array-list ByteArrayList :int8)
(bind-array-list ArrayLists$ShortArraySubList :int16)
(bind-array-list ShortArrayList :int16)
(bind-array-list ArrayLists$CharArraySubList :char)
(bind-array-list CharArrayList :char)
(bind-array-list ArrayLists$IntArraySubList :int32)
(bind-array-list ArrayLists$IntArrayList :int32)
(bind-array-list ArrayLists$LongArraySubList :int64)
(bind-array-list ArrayLists$LongArrayList :int64)
(bind-array-list ArrayLists$FloatArraySubList :float32)
(bind-array-list FloatArrayList :float32)
(bind-array-list ArrayLists$DoubleArraySubList :float64)
(bind-array-list ArrayLists$DoubleArrayList :float64)


(defmacro ^:private host->long-uint8
  [v]
  `(Byte/toUnsignedInt ~v))

(defmacro ^:private long->host-uint8
  [v]
  `(NumericConversions/ubyteHostCast ~v))

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
     (cloneList [this#] (~(symbol (str (name list-name) "."))
                         (.copyOf this# ~'n-elems)
                         ~'sidx ~'n-elems ~'m))
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
(bind-array-list UByteArraySubList :uint8)
(extend-type UByteArraySubList
  dtype-proto/PToBuffer
  (convertible-to-buffer? [this] true)
  (->buffer [this] (UByteSubBuffer. (.-data this)
                                    (.-sidx this)
                                    (+ (.-sidx this) (.-n-elems this))
                                    (meta this))))

;;UByte buffers get special treatment because they are used so often as image
;;backing data.
(extend-type UByteSubBuffer
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [this] :uint8)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [this new-dtype] this))

(extend-array-owner! UByteSubBuffer)

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
     (cloneList [this#] (~(symbol (str (name lname) ".")) (.copyOf this# ~'n-elems)
                         ~'n-elems ~'m))
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
           (reduce (hamf/long-accumulator
                    acc# v# (.addLong ^IMutList acc# v#) acc#)
                   this#
                   c#))
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
  (when-not (or (not (casting/numeric-type? ary-dtype))
                (identical? ary-dtype buf-dtype)
                (identical? (casting/datatype->host-datatype ary-dtype)
                            (casting/datatype->host-datatype buf-dtype)))
    (throw (Exception. (str "Array datatype " ary-dtype " and buffer datatype " buf-dtype
                            " are not compatible"))))
  buf-dtype)


(defn array-sub-list
  (^IMutList [dtype]
   (array-sub-list dtype 0))
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
             retlist (case dtype
                       :uint8 (UByteArraySubList. src-data 0 n-elems m)
                       :uint16 (UShortArraySubList. src-data 0 n-elems m)
                       :uint32 (UIntArraySubList. src-data 0 n-elems m)
                       :uint64 (ULongArraySubList. src-data 0 n-elems m)
                       (ArrayLists/toList src-data))
             retval (array-list->packed-list retlist dtype)]
         (when (instance? RandomAccess data)
           (.fillRange ^IMutList retval 0 data))
         retval)
       (let [alist (array-list dtype 0)]
         (.addAllReducible ^IMutList alist data)
         (hamf/subvec alist 0)))))
  (^IMutList [dtype data sidx eidx m]
   (ensure-datatypes (dtype-proto/elemwise-datatype data) dtype)
   (-> (let [ne (- (long eidx) (long sidx))]
         (case dtype
           :uint8 (UByteArraySubList. data sidx ne m)
           :uint16 (UShortArraySubList. data sidx ne m)
           :uint32 (UIntArraySubList. data sidx ne m)
           :uint64 (ULongArraySubList. data sidx ne m)
           (ArrayLists/toList data (long sidx) (long eidx) ^IPersistentMap m)))
       (array-list->packed-list dtype))))


(defn as-growable-list
  ^IMutList [data ^long ptr]
  (when-not (dtype-proto/convertible-to-array-buffer? data)
    (throw (RuntimeException. "Buffer not convertible to array buffer")))
  (let [^ArrayBuffer abuf (dtype-proto/->array-buffer data)]
    (when-not (== 0 (.-offset abuf))
      (throw (RuntimeException. "Only non-sub-buffer containers can become growable lists.")))
    (when-not (<= ptr (.-n-elems abuf))
      (throw (RuntimeException. "ptr out of range of buffer size")))
    (let [dtype (dtype-proto/elemwise-datatype abuf)
          host-dt (if (packing/packed-datatype? dtype)
                    (casting/datatype->host-datatype dtype)
                    dtype)]
      (-> (case host-dt
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
            (ArrayLists$ObjectArrayList. (.-ary-data abuf) ptr (meta data)))
          ;;Add packing as an optional second layer
          (array-list->packed-list dtype)))))


(defn array-list
  (^IMutList [dtype]
   (array-list dtype 4))
  (^IMutList [dtype data]
   (let [rdr? (dtype-proto/convertible-to-reader? data)
         data (if rdr?
                (dtype-proto/->reader data)
                data)
         n-elems (cond
                   (number? data)
                   (long data)
                   rdr?
                   (dtype-proto/ecount data)
                   :else
                   8)
         c (array-sub-list dtype n-elems)
         ptr (if (number? data)
               0
               (dtype-proto/ecount c))
         retval (as-growable-list c ptr)]
     (when-not (number? data)
       (.addAllReducible ^IMutList retval data))
     retval)))



(defn array-buffer
  ([java-ary]
   (array-buffer java-ary (dtype-proto/elemwise-datatype java-ary)))
  ([java-ary buf-dtype]
   (let [ary-dtype (dtype-proto/elemwise-datatype java-ary)]
     (ArrayBuffer. (ensure-array java-ary) 0 (Array/getLength java-ary)
                   (ensure-datatypes ary-dtype buf-dtype) nil nil))))


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


(defn- implement-array!
  [dtype len-fn]
  (let [ary-cls (typecast/datatype->array-cls dtype)
        array-dtype (keyword (str (name dtype) "-array"))
        ary->buffer (fn [ary]
                      (MutListBuffer. (hamf/->random-access ary) true
                                      (dtype-proto/elemwise-datatype ary)))]
    (extend ary-cls
      dtype-proto/PElemwiseDatatype
      {:elemwise-datatype (if (identical? dtype :object)
                            (fn [^objects ary]
                              (-> (.getClass ary)
                                  (.getComponentType)
                                  (casting/object-class->datatype)))
                            (constantly dtype))}
      dtype-proto/PElemwiseReaderCast
      {:elemwise-reader-cast ml-reader-cast}
      dtype-proto/PDatatype
      {:datatype (constantly array-dtype)}
      dtype-proto/PECount
      {:ecount len-fn}
      dtype-proto/PEndianness
      {:endianness (constantly :little-endian)}
      dtype-proto/PToArrayBuffer
      {:convertible-to-array-buffer? (constantly true)
       :->array-buffer (fn [ary]
                         (ArrayBuffer. ary 0
                                       (len-fn ary)
                                       (dtype-proto/elemwise-datatype ary)
                                       nil nil))}
      dtype-proto/PSubBuffer
      {:sub-buffer (fn [ary ^long off ^long len]
                     (hamf/subvec ary off (+ off len)))}
      dtype-proto/PCopyRawData
      {:copy-raw->item! array-buffer-convertible-copy-raw-data}
      dtype-proto/PToBuffer
      {:convertible-to-buffer? (constantly true)
       :->buffer ary->buffer}
      dtype-proto/PToReader
      {:convertible-to-reader? (constantly true)
       :->reader ary->buffer}
      dtype-proto/PToWriter
      {:convertible-to-writer? (constantly true)
       :->writer ary->buffer})))


(defmacro initial-implement-arrays
  []
  `(do
     ~@(->>
        array-types
        (map
         (fn [ary-type]
           `(implement-array! ~ary-type #(alength (typecast/datatype->array ~ary-type %))))))))


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
