(ns tech.v3.datatype.hamf
  "Bindings to the various ham-fisted containers."
  (:require [ham-fisted.api :as hamf]
            [ham-fisted.iterator :as iterator]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc])
  (:import [ham_fisted ArrayLists ArrayLists$ArrayOwner ArrayLists$ILongArrayList
            ArrayLists$ObjectArraySubList ArrayLists$ObjectArrayList
            ArrayLists$ByteArraySubList ArrayLists$ShortArraySubList
            ArrayLists$IntArraySubList ArrayLists$IntArrayList
            ArrayLists$LongArraySubList ArrayLists$LongArrayList
            ArrayLists$FloatArraySubList ArrayLists$CharArraySubList
            ArrayLists$DoubleArraySubList ArrayLists$DoubleArrayList
            ArrayLists$BooleanArraySubList ArrayLists$ILongArrayList
            ArrayLists$ArraySection Transformables ArrayHelpers
            LongMutList Casts IMutList]
           [tech.v3.datatype MutListBuffer]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [ham_fisted.alists ByteArrayList ShortArrayList CharArrayList
            BooleanArrayList FloatArrayList]
           [clojure.lang IPersistentMap IReduceInit]
           [java.util Arrays RandomAccess List]))

(set! *warn-on-reflection* true)


(defmacro bind-array-list
  [alist-type ewise-dtype]
  `(extend-type ~alist-type
     dtype-proto/PDatatype
     (datatype [this#] :array-buffer)
     dtype-proto/PElemwiseDatatype
     (elemwise-datatype [~'this] ~ewise-dtype)
     dtype-proto/PElemwiseDatatype
     (elemwise-datatype [~'this] ~ewise-dtype)
     dtype-proto/PElemwiseReaderCast
     (elemwise-reader-cast [this# new-dtype#] this#)
     dtype-proto/PSubBuffer
     (sub-buffer [this# offset# length#]
       (.subList this# offset# (+ offset# length#)))
     dtype-proto/PClone
     (clone [this#] (.cloneList this#))
     dtype-proto/PSetConstant
     (set-constant! [this# offset# elem-count# value#]
       (.fillRange this# offset# (+ offset# elem-count#) value#))
     dtype-proto/PToBuffer
     (convertible-to-buffer? [this#] true)
     (->buffer [this#]
       (MutListBuffer. this# true (dtype-proto/elemwise-datatype this#)))
     dtype-proto/PToArrayBuffer
     (convertible-to-array-buffer? [buf#] true)
     (->array-buffer [buf#]
       (let [section# (.getArraySection buf#)
             sidx# (.-sidx section#)
             eidx# (.-eidx section#)]
         (ArrayBuffer. (.-array section#) sidx# (- eidx# sidx#)
                       (dtype-proto/elemwise-datatype buf#)
                       (meta buf#) (dtype-proto/->buffer buf#))))
     dtype-proto/PCopyRawData
     (copy-raw->item! [this# ary-target# ary-offset# options#]
       (dtype-cmc/copy! (dtype-proto/->array-buffer this#)
                        (dtype-proto/sub-buffer ary-target# ary-offset#
                                                (dtype-proto/ecount this#))))))


(bind-array-list ArrayLists$ObjectArraySubList
                 '(casting/object-class->datatype (.containedType this)))
(bind-array-list ArrayLists$ObjectArrayList
                 '(casting/object-class->datatype (.containedType this)))
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
       (ArrayLists/checkIndexRange ~'n-elems ssidx# seidx#)
       (~(symbol (str list-name ".")) ~'data (+ ~'sidx ssidx#) (- ~'sidx seidx#) ~'m))
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
       (ArrayLists$ArraySection. ~'data ~'sidx (+ ~'sidx ~'n-elems)))))


(make-unsigned-sub-list UByteArraySubList bytes
                        host->long-uint8
                        long->host-uint8
                        object->host-uint8)
(bind-array-list UByteArraySubList :uint8)

(make-unsigned-sub-list UShortArraySubList shorts
                        host->long-uint16
                        long->host-uint16
                        object->host-uint16)
(bind-array-list UShortArraySubList :uint16)

(make-unsigned-sub-list UIntArraySubList ints
                        host->long-uint32
                        long->host-uint32
                        object->host-uint32)
(bind-array-list UIntArraySubList :uint32)

(make-unsigned-sub-list ULongArraySubList longs
                        host->long-uint64
                        long->host-uint64
                        object->host-uint64)
(bind-array-list ULongArraySubList :uint64)


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
       (ArrayLists/checkIndexRange ~'n-elems sidx# eidx#)
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
         (cond
           (instance? RandomAccess c#)
           (do
             (let [~(with-meta 'c {:tag 'List}) c#
                   curlen# ~'n-elems
                   newlen# (+ curlen# (.size ~'c))]
               (.ensureCapacity this# newlen#)
               (set! ~'n-elems newlen#)
               (.fillRange this# curlen# ~'c)))
           (instance? IReduceInit c#)
           (reduce #(do (.add this# %2) %1) this# c#)
           :else
           (iterator/doiter obj# c# (.add this# obj#)))
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
       (ArrayLists$ArraySection. ~'data 0 ~'n-elems))))


(make-unsigned-list UByteArrayList bytes host->long-uint8 long->host-uint8
                    object->host-uint8 UByteArraySubList)
(bind-array-list UByteArrayList :uint8)

(make-unsigned-list UShortArrayList shorts host->long-uint16 long->host-uint16
                    object->host-uint16 UShortArraySubList)
(bind-array-list UShortArrayList :uint16)

(make-unsigned-list UIntArrayList ints host->long-uint32 long->host-uint32
                    object->host-uint32 UIntArraySubList)
(bind-array-list UIntArrayList :uint32)

(make-unsigned-list ULongArrayList longs host->long-uint64 long->host-uint64
                    object->host-uint64 ULongArraySubList)
(bind-array-list ULongArrayList :uint64)


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
         (hamf/subvec alist 0))))))


(defn array-list
  (^IMutList [dtype] (array-list dtype 0))
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
