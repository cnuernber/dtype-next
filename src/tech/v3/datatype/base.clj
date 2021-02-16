(ns tech.v3.datatype.base
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.io-sub-buffer :as io-sub-buf]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.parallel.for :as parallel-for])
  (:import [tech.v3.datatype Buffer BinaryBuffer
            ObjectBuffer ElemwiseDatatype ObjectReader NDBuffer
            LongReader DoubleReader]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [clojure.lang IPersistentCollection APersistentMap APersistentVector
            APersistentSet]
           [java.util RandomAccess List
            Spliterator Spliterator$OfDouble Spliterator$OfLong
            Spliterator$OfInt]
           [java.util.stream Stream DoubleStream LongStream IntStream]))


(set! *warn-on-reflection* true)


(defn elemwise-datatype
  "Return the datatype one would expect when iterating through a container
  of this type.  For scalars, return your elemental datatype."
  [item]
  ;;false has a datatype in this world.
  (if-not (nil? item)
    (dtype-proto/elemwise-datatype item)
    :object))


(defn datatype
  "Return this object's actual datatype.
  **This is not the same as the DEPRECATED get-datatype.  That is function maps
  to elemwise-datatype.**
  This maps to the parameterization of the object, so for instance a list of ints
  might be:
```clojure
  {:container-type :list :elemwise-datatype :int32}
```
  Defaults to this object's elemwise-datatype."
  [item]
  (if-not (nil? item)
    (dtype-proto/datatype item)
    :object))


(defn elemwise-cast
  "Create a new thing by doing a checked cast from the old values
  to the new values upon at read time.  Return value will report
  new-dtype as it's elemwise-datatype."
  [item new-dtype]
  (when-not (nil? item)
    (if-not (= new-dtype (dtype-proto/elemwise-datatype new-dtype))
      (dtype-proto/elemwise-cast item new-dtype)
      item)))


(defn ecount
  "Return the long number of elements in the object."
  ^long [item]
  (if-not item
    0
    (dtype-proto/ecount item)))


(defn shape
  "Return a persistent vector of the shape of the object.  Dimensions
  are returned from least-rapidly-changing to most-rapidly-changing."
  [item]
  (if-not item
    nil
    (dtype-proto/shape item)))


(defn as-buffer
  "If this item is or has a conversion to an implementation of the Buffer
  interface then return the buffer interface for this object.
  Else return nil."
  ^Buffer [item]
  (when-not item (throw (Exception. "Cannot convert nil to reader")))
  (if (instance? Buffer item)
    item
    (when (dtype-proto/convertible-to-buffer? item)
      (dtype-proto/->buffer item))))


(defn ->buffer
  "Convert this item to a buffer implementation or throw an exception."
  ^Buffer [item]
  (if-let [io (as-buffer item)]
    io
    (errors/throwf "Item type %s is not convertible to buffer"
                   (type item))))

(defn as-reader
  "If this object has a read-only or read-write conversion to a primitive
  io object return the buffer object."
  (^Buffer [item]
   (cond
     (nil? item) item
     (and (instance? Buffer item)
          (.allowsRead ^Buffer item))
     item
     :else
     (when (dtype-proto/convertible-to-reader? item)
       (dtype-proto/->reader item))))
  (^Buffer [item read-datatype]
   (cond
     (nil? item) item
     (= read-datatype (dtype-proto/elemwise-datatype item))
     (as-reader item)
     :else
     (dtype-proto/elemwise-reader-cast item read-datatype))))

(defn ->reader
  "If this object has a read-only or read-write conversion to a buffer
  object return the buffer object.  Else throw an exception."
  (^Buffer [item]
   (if-let [io (as-reader item)]
     io
     (errors/throwf "Item type %s is not convertible to primitive reader"
                    (type item))))
  (^Buffer [item read-datatype]
   (if-let [io (as-reader item read-datatype)]
     io
     (errors/throwf "Item type %s is not convertible to primitive reader"
                    (type item)))))

(defn reader?
  "True if this item is convertible to a read-only or read-write Buffer
  interface."
  [item]
  (when item (dtype-proto/convertible-to-reader? item)))

(defn ->iterable
  "Ensure this object is iterable.  If item is iterable, return the item.
  If the item is convertible to a buffer object , convert to it and
  return the object.  Else if the item is a scalar constant, return
  an infinite sequence of items."
  (^Iterable [item]
   (cond
     (instance? Iterable item)
     item
     (instance? Stream item)
     (reify Iterable
       (iterator [item]
         (.iterator ^Stream item)))
     (reader? item)
     (->reader item)
     :else
     (let [item-dtype (dtype-proto/elemwise-datatype item)]
       (reify
         Iterable
         (iterator [it]
           (.iterator ^Iterable (repeat item)))
         ElemwiseDatatype
         (elemwiseDatatype [it] item-dtype))))))


(defn iterable?
  "Return true if this is either a instance of Iterable or an instance of
  java.util.streams.Stream."
  [item]
  (or (instance? Iterable item)
      (instance? Stream item)
      (reader? item)))


(defn writer?
  "True if this item is convertible to a Buffer interface that supports
  writing."
  [item]
  (when item
    (dtype-proto/convertible-to-writer? item)))


(defn as-writer
  "If this item is convertible to a PrimtiveIO writing the perform the
  conversion and return the interface."
  ^Buffer [item]
  (when-not item (throw (Exception. "Cannot convert nil to writer")))
  (if (and (instance? Buffer item)
           (.allowsWrite ^Buffer item))
    item
    (when (dtype-proto/convertible-to-writer? item)
      (dtype-proto/->writer item))))


(defn ->writer
  "If this item is convertible to a PrimtiveIO writing the perform the
  conversion and return the interface.
  Else throw an excepion."
  ^Buffer [item]
  (if-let [io (as-writer item)]
    io
    (errors/throwf "Item type %s is not convertible to primitive writer"
                   (type item))))


(defn as-binary-buffer
  "Convert an item into a binary buffer.  Fails with nil."
  ^BinaryBuffer [item]
  (when item
    (if (instance? BinaryBuffer item) item
        (when (dtype-proto/convertible-to-binary-buffer? item)
          (dtype-proto/->binary-buffer item)))))


(defn ->binary-buffer
  "Convert an item into a binary buffer. Fails with an exception."
  ^BinaryBuffer [item]
  (let [retval (as-binary-buffer item)]
    (errors/when-not-errorf
     item
     "Item %s is not convertible to a binary buffer" item)
    retval))


(defn as-array-buffer
  "If this item is convertible to a tech.v3.datatype.array_buffer.ArrayBuffer
  then convert it and return the typed buffer"
  ^ArrayBuffer [item]
  (if (instance? ArrayBuffer item)
    item
    (when (dtype-proto/convertible-to-array-buffer? item)
      (dtype-proto/->array-buffer item))))


(defn as-native-buffer
  "If this item is convertible to a tech.v3.datatype.native_buffer.NativeBuffer
  then convert it and return the typed buffer"
  ^NativeBuffer [item]
  (if (instance? NativeBuffer item)
    item
    (when (dtype-proto/convertible-to-native-buffer? item)
      (dtype-proto/->native-buffer item))))


(defn ->native-buffer
  "Convert to a native buffer if possible, else throw an exception.
  See as-native-buffer"
  ^NativeBuffer [item]
  (if-let [retval (as-native-buffer item)]
    retval
    (errors/throwf "Item type %s is not convertible to an native buffer"
                   (type item))))


(defn as-concrete-buffer
  "If this item is representable as a base buffer type, either native or array,
  return that buffer."
  [item]
  (or (as-array-buffer item)
      (as-native-buffer item)))


(defn sub-buffer
  "Create a sub-buffer (view) of the given buffer."
  ([item offset length]
   (errors/check-offset-length offset length (ecount item))
   (dtype-proto/sub-buffer item offset length))
  ([item ^long offset]
   (let [n-elems (ecount item)]
     (sub-buffer item offset (- n-elems offset)))))


(defn get-value
  "Get a value from an object via conversion to a reader."
  [item idx]
  ((->reader item) idx))


(defn set-value!
  "Set a value on an object via conversion to a writer."
  [item idx value]
  ((->writer item) idx value))


(defn- random-access->io
  [^List item]
  (reify
    ObjectBuffer
    (elemwiseDatatype [rdr] :object)
    (lsize [rdr] (long (.size item)))
    (readObject [rdr idx]
      (.get item idx))
    (writeObject [wtr idx value]
      (.set item idx value))))


(extend-type RandomAccess
  dtype-proto/PDatatype
  (datatype [item] :random-access-list)
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] (random-access->io item))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item]
    (random-access->io item))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] (boolean (not (instance? IPersistentCollection item))))
  (->writer [item]
    (when (instance? IPersistentCollection item)
      (throw (Exception. "Item is a persistent collection and thus not convertible to writer")))
    (random-access->io item)))


(extend-protocol dtype-proto/PElemwiseDatatype
  DoubleStream
  (elemwise-datatype [item] :float64)
  Spliterator$OfDouble
  (elemwise-datatype [item] :float64)
  LongStream
  (elemwise-datatype [item] :int64)
  Spliterator$OfLong
  (elemwise-datatype [item] :int64)
  IntStream
  (elemwise-datatype [item] :int32)
  Spliterator$OfInt
  (elemwise-datatype [item] :int32))


(extend-protocol dtype-proto/PDatatype
  Stream
  (datatype [item] :stream)
  DoubleStream
  (datatype [item] :stream)
  Spliterator$OfDouble
  (datatype [item] :spliterator)
  LongStream
  (datatype [item] :stream)
  Spliterator$OfLong
  (datatype [item] :spliterator)
  IntStream
  (datatype [item] :stream)
  Spliterator$OfInt
  (datatype [item] :spliterator))


(defn- inner-buffer-sub-buffer
  [buf ^long offset ^Long len]
  (when-let [data-buf (as-concrete-buffer buf)]
    (with-meta
      (sub-buffer data-buf offset len)
      (meta buf))))


(extend-type Buffer
  dtype-proto/PDatatype
  (datatype [item] :buffer)
  dtype-proto/PShape
  (shape [item] [(.lsize item)])
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype]
    ;;The buffer implementations themselves have casting in their
    ;;type specific get methods so this cast cannot possibly do anything.
    item)
  dtype-proto/PToBuffer
  (convertible-to-buffer? [buf] true)
  (->buffer [item] item)
  dtype-proto/PToReader
  (convertible-to-reader? [buf] true)
  (->reader [buf] buf)
  dtype-proto/PToWriter
  (convertible-to-writer? [buf] true)
  (->writer [buf] buf)
  dtype-proto/PClone
  (clone [buf]
    (dtype-proto/make-container :jvm-heap (elemwise-datatype buf) {} buf))
  dtype-proto/PSetConstant
  (set-constant! [buf offset elem-count value]
    (errors/check-offset-length offset elem-count (ecount buf))
    (let [value (casting/cast value (elemwise-datatype buf))]
      (parallel-for/parallel-for
       idx (long elem-count)
       (.writeObject buf idx value)))
    buf))

(defn array?
  "Return true if this object's class returns true for .isArray."
  [item]
  (when item
    (let [cls (.getClass ^Object item)]
      (.isArray cls))))


(defn scalar?
  "Return true if this is a scalar object."
  [item]
  (or (number? item)
      (string? item)
      (and
       (not (array? item))
       (not (iterable? item))
       (not (reader? item)))))

(extend-type Class
  dtype-proto/PDatatype
  (datatype [cls]
    (.getOrDefault casting/class->datatype-map cls :object)))


(defn nested-array-elemwise-datatype
  [item]
  (when item
    (loop [item-cls (.getClass ^Object item)]
      (let [^Class comp-type (.getComponentType item-cls)]
        (if (and comp-type (.isArray comp-type))
          (recur comp-type)
          (dtype-proto/datatype comp-type))))))


(defn shape-ary->stride-ary
  "Strides assuming everything is increasing and packed."
  ^longs [shape-vec]
  (let [retval (long-array (count shape-vec))
        shape-vec (->buffer shape-vec)
        n-elems (alength retval)
        n-elems-dec (dec n-elems)]
    (loop [idx n-elems-dec
           last-stride 1]
      (if (>= idx 0)
        (let [next-stride (* last-stride
                             (if (== idx n-elems-dec)
                               1
                               (.readLong shape-vec (inc idx))))]
          (aset retval idx next-stride)
          (recur (dec idx) next-stride))
        retval))))

(def byte-array-class (Class/forName "[B"))
(def short-array-class (Class/forName "[S"))
(def char-array-class (Class/forName "[C"))
(def int-array-class (Class/forName "[I"))
(def long-array-class (Class/forName "[J"))
(def float-array-class (Class/forName "[F"))
(def double-array-class (Class/forName "[D"))


(defn make-nested-array-get-value-fn
  [narray]
  (let [ary-shape (shape narray)
        n-shape (dec (count ary-shape))
        barray-class (loop [narray-cls (.getClass ^Object narray)
                            idx 0]
                       (if (< idx n-shape)
                         (recur (.getComponentType ^Class narray-cls)
                                (unchecked-inc idx))
                         narray-cls))]
    (cond
      (identical? byte-array-class barray-class)
      (fn [^bytes barray ^long idx]
        (aget barray idx))
      (identical? char-array-class barray-class)
      (fn [^chars barray ^long idx]
        (aget barray idx))
      (identical? short-array-class barray-class)
      (fn [^shorts barray ^long idx]
        (aget barray idx))
      (identical? int-array-class barray-class)
      (fn [^ints barray ^long idx]
        (aget barray idx))
      (identical? long-array-class barray-class)
      (fn [^longs barray ^long idx]
        (aget barray idx))
      (identical? float-array-class barray-class)
      (fn [^floats barray ^long idx]
        (aget barray idx))
      (identical? double-array-class barray-class)
      (fn [^doubles barray ^long idx]
        (aget barray idx))
      :else
      (fn [^"[Ljava.lang.Object;" barray ^long idx]
        (aget barray idx)))))


(defn rectangular-nested-array->elemwise-reader
  [narray]
  (let [^"[Ljava.lang.Object;" narray narray
        ary-shape (int-array (shape narray))
        ary-strides (shape-ary->stride-ary ary-shape)
        n-elems (long (apply * ary-shape))
        n-strides (alength ary-strides)
        d-n-strides (dec n-strides)
        gval-fn (make-nested-array-get-value-fn narray)
        edtype (nested-array-elemwise-datatype narray)]
    (case (count ary-shape)
      2 (let [n-x (long (aget ary-strides 0))]
          (reify ObjectReader
            (elemwiseDatatype [rdr] edtype)
            (lsize [rdr] n-elems)
            (readObject [rdr idx]
              (let [barray (aget narray (quot idx n-x))]
                (gval-fn barray (rem idx n-x))))))
      3 (let [n-y (long (aget ary-strides 0))
              n-x (long (aget ary-strides 1))]
          (reify ObjectReader
            (elemwiseDatatype [rdr] edtype)
            (lsize [rdr] n-elems)
            (readObject [rdr idx]
              (let [^"[Ljava.lang.Object;" narray (aget narray (quot idx n-y))
                    leftover (rem idx n-y)
                    barray (aget narray (quot leftover n-x))]
                (gval-fn barray (rem leftover n-x))))))
      (reify ObjectReader
        (elemwiseDatatype [rdr] edtype)
        (lsize [rdr] n-elems)
        (readObject [rdr idx]
          (let [n-x (aget ary-strides (long (dec d-n-strides)))
                chan (rem idx n-x)
                barray
                (loop [stride-idx 0
                       narray narray
                       idx idx]
                  (let [stride (aget ary-strides stride-idx)
                        ary-idx (quot idx stride)
                        idx (rem idx stride)]
                    (if (< stride-idx d-n-strides)
                      (recur (unchecked-inc stride-idx)
                             (aget ^"[Ljava.lang.Object;" narray ary-idx)
                             idx)
                      narray)))]
            (gval-fn barray chan)))))))

(defn rectangular-nested-array->array-reader
  ^Buffer [narray]
  (let [^"[Ljava.lang.Object;" narray narray
        ary-shape (int-array (drop-last (shape narray)))
        ary-strides (shape-ary->stride-ary ary-shape)
        n-elems (long (apply * ary-shape))
        n-strides (alength ary-strides)
        d-n-strides (dec n-strides)]
    (case (count ary-shape)
      1 (reify ObjectReader
          (lsize [rdr] n-elems)
          (readObject [rdr idx]
            (aget narray idx)))
      2 (let [n-x (long (aget ary-strides 1))]
          (reify ObjectReader
            (lsize [rdr] n-elems)
            (readObject [rdr idx]
              (let [^"[Ljava.lang.Object;" narray (aget narray (quot idx n-x))
                    leftover (rem idx n-x)
                    barray (aget narray (quot leftover n-x))]
                barray))))
      (reify ObjectReader
        (lsize [rdr] n-elems)
        (readObject [rdr idx]
          (let [n-x (aget ary-strides (long (dec d-n-strides)))
                chan (rem idx n-x)
                barray
                (loop [stride-idx 0
                       narray narray
                       idx idx]
                  (let [stride (aget ary-strides stride-idx)
                        ary-idx (quot idx stride)
                        idx (rem idx stride)]
                    (if (< stride-idx d-n-strides)
                      (recur (unchecked-inc stride-idx)
                             (aget ^"[Ljava.lang.Object;" narray ary-idx)
                             idx)
                      narray)))]
            (aget ^"[Ljava.lang.Object;" barray chan)))))))


;;Datatype library Object defaults.  Here lie dragons.
(extend-type Object
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [item] :object)
  dtype-proto/PDatatype
  (datatype [item]
    (if-let [buffer (as-concrete-buffer item)]
      (dtype-proto/datatype buffer)
      (dtype-proto/elemwise-datatype item)))
  dtype-proto/PElemwiseCast
  (elemwise-cast [item new-dtype]
    (let [src-dtype (dtype-proto/elemwise-datatype item)]
      (cond
        (= new-dtype (packing/pack-datatype src-dtype))
        (packing/pack item)
        (= new-dtype (packing/unpack-datatype src-dtype))
        (packing/unpack item)
        :else
        (let [cast-fn #(casting/cast % new-dtype)]
          (dispatch/vectorized-dispatch-1
           cast-fn
           (fn [op-dtype item]
             (dispatch/typed-map-1 cast-fn new-dtype item))
           (fn [op-dtype item]
             (case (casting/simple-operation-space new-dtype
                                                   (elemwise-datatype item))
               :int64
               (let [src-rdr (dtype-proto/elemwise-reader-cast item :int64)]
                 (reify LongReader
                   (elemwiseDatatype [rdr] new-dtype)
                   (lsize [rdr] (.lsize src-rdr))
                   (readLong [rdr idx] (.readLong src-rdr idx))))
               :float64
               (let [src-rdr (dtype-proto/elemwise-reader-cast item :float64)]
                 (reify DoubleReader
                   (elemwiseDatatype [rdr] new-dtype)
                   (lsize [rdr] (.lsize src-rdr))
                   (readDouble [rdr idx] (.readDouble src-rdr idx))))
               (let [src-rdr (->reader item)]
                 (reify ObjectReader
                   (elemwiseDatatype [rdr] new-dtype)
                   (lsize [rdr] (.lsize src-rdr))
                   (readObject [rdr idx]
                     (cast-fn (.readObject src-rdr idx)))))))
           item)))))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype]
    (if (array? item)
      (->reader item)
      (as-reader (elemwise-cast item new-dtype))))
  dtype-proto/PECount
  (ecount [item] (count item))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [buf] (.isArray (.getClass ^Object buf)))
  (->array-buffer [buf]
    (when-not (.isArray (.getClass ^Object buf))
      (throw (Exception. "Item is not an array: %s" (type buf))))
    (array-buffer/array-buffer buf))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [buf]
    (or (dtype-proto/convertible-to-array-buffer? buf)
        (dtype-proto/convertible-to-native-buffer? buf)
        (.isArray (.getClass ^Object buf))))
  (->buffer [buf]
    (if-let [buf-data (as-concrete-buffer buf)]
      (dtype-proto/->buffer buf-data)
      (if (array? buf)
        (dtype-proto/->buffer (array-buffer/array-buffer buf))
        (errors/throwf "Buffer type %s is not convertible to buffer"
                       (type buf)))))
  dtype-proto/PToReader
  (convertible-to-reader? [buf]
    (dtype-proto/convertible-to-buffer? buf))
  (->reader [buf]
    (dtype-proto/->buffer buf))
  dtype-proto/PToWriter
  (convertible-to-writer? [buf]
    (if-not (instance? IPersistentCollection buf)
      (dtype-proto/convertible-to-buffer? buf)
      false))
  (->writer [buf]
    (dtype-proto/->buffer buf))
  dtype-proto/PSubBuffer
  (sub-buffer [buf offset len]
    (if-let [data-buf (inner-buffer-sub-buffer buf offset len)]
      data-buf
      (if-let [data-io (->buffer buf)]
        (io-sub-buf/sub-buffer data-io offset len)
        (throw (Exception. (format
                            "Buffer %s does not implement the sub-buffer protocol"
                            (type buf)))))))
  dtype-proto/PSetConstant
  (set-constant! [buf offset element-count value]
    (errors/check-offset-length offset element-count (ecount buf))
    (if-let [buf-data (as-concrete-buffer buf)]
      ;;highest performance
      (dtype-proto/set-constant! buf-data offset element-count value)
      (if-let [writer (->writer buf)]
        (dtype-proto/set-constant! writer offset element-count value)
        (throw
         (Exception.
          (format "Buffer is not convertible to writer, array or native buffer: %s"
                  (type buf))))))
    buf)
  ;;Note the lack of reflection here now.  The standard way is to use reflection to
  ;;find a clone method and this of course breaks graal native.
  dtype-proto/PClone
  (clone [buf]
    (if-let [buf-data (as-concrete-buffer buf)]
      ;;highest performance
      (dtype-proto/clone buf-data)
      (if-let [rdr (->reader buf)]
        (dtype-proto/clone rdr)
        (throw
         (Exception.
          (format "Buffer is not cloneable: %s"
                  (type buf)))))))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [buf] false)
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [buf] false)
  dtype-proto/PRangeConvertible
  (convertible-to-range? [buf] false)
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [buf] false)
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [item] false)
  dtype-proto/POperator
  (op-name [item] :_unnamed)
  dtype-proto/PShape
  (shape [item]
    (cond
      (scalar? item)
      nil
      (array? item)
      (let [n-elems (count item)]
        (-> (concat [n-elems]
                    (when (> n-elems 0)
                      (let [first-elem (first item)]
                        (shape first-elem))))
            vec))
      :else
      [(dtype-proto/ecount item)])))


(extend-type IPersistentCollection
  dtype-proto/PClone
  (clone [buf] buf)
  dtype-proto/PToWriter
  (convertible-to-writer? [buf] false))


(extend-type List
  dtype-proto/PShape
  (shape [item]
    (let [fitem (first item)]
      (if (or (reader? fitem)
              (sequential? fitem))
        (->> (concat [(.size item)]
                     (dtype-proto/shape fitem))
             vec)
        [(.size item)]))))


(extend-type Iterable
  dtype-proto/PDatatype
  (datatype [item] :iterable))


(extend-type APersistentMap
  dtype-proto/PDatatype
  (datatype [item] :persistent-map))

(casting/add-object-datatype! :persistent-map APersistentMap false)

(extend-type APersistentVector
  dtype-proto/PDatatype
  (datatype [item] :persistent-vector))

(casting/add-object-datatype! :persistent-vector APersistentVector false)

(extend-type APersistentSet
  dtype-proto/PDatatype
  (datatype [item] :persistent-set))

(casting/add-object-datatype! :persistent-set APersistentSet false)


(defn set-constant!
  "Set a contiguous region of a buffer to a constant value"
  ([item ^long offset ^long length value]
   (errors/check-offset-length offset length (ecount item))
   (dtype-proto/set-constant! item offset length value))
  ([item offset value]
   (set-constant! item offset (- (ecount item) (long offset)) value))
  ([item value]
   (set-constant! item 0 (ecount item) value)))


(defn- check-ns
  [namespace]
  (errors/when-not-errorf
   (find-ns namespace)
   "Required namespace %s is missing." namespace))


(defn reshape
  "Reshape this item into a new shape.  For this to work, the tensor
  namespace must be required.
  Always returns a tensor."
  ^NDBuffer [t new-shape]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/reshape t new-shape))


(defn select
  "Perform a subrect projection or selection
  Shape arguments may be readers, ranges, integers, or the keywords [:all :lla].
  :all means take the entire dimension, :lla means reverse the dimension.
  Arguments are applied left to right and any missing arguments are assumed to
  be :all.

  Example:

  ```clojure
  user> (dtt/select (dtt/->tensor [1 2 3]) [0 2])
  #tech.v3.tensor<object>[2]
  [1 3]

  user> (def tensor (dtt/->tensor
              [[1 2 3]
               [4 5 6]]))
  #'user/tensor
  user> (dtt/select tensor [1] [0 2])
  #tech.v3.tensor<object>[2]
  [4 6]
  user> (dtt/select tensor [0 1] [1 2])
  #tech.v3.tensor<object>[2 2]
  [[2 3]
   [5 6]]
  ```"
  ^NDBuffer [t & new-shape]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/select t new-shape))


(defn transpose
  "In-place transpose an n-d object into a new shape.  Returns a tensor.

  reorder-indexes are the relative indexes of the old indexes

  Example:

```clojure
user> (def tensor (dtt/->tensor (partition 2 (partition 3 (flatten (repeat 6 [:r :g :b]))))))
#'user/tensor
user> tensor
#tech.v3.tensor<object>[3 2 3]
[[[:r :g :b]
  [:r :g :b]]
 [[:r :g :b]
  [:r :g :b]]
 [[:r :g :b]
  [:r :g :b]]]
user> (dtt/transpose tensor [2 0 1])
#tech.v3.tensor<object>[3 3 2]
[[[:r :r]
  [:r :r]
  [:r :r]]
 [[:g :g]
  [:g :g]
  [:g :g]]
 [[:b :b]
  [:b :b]
  [:b :b]]]
user> (dtt/transpose tensor [1 2 0])
#tech.v3.tensor<object>[2 3 3]
[[[:r :r :r]
  [:g :g :g]
  [:b :b :b]]
 [[:r :r :r]
  [:g :g :g]
  [:b :b :b]]]
```"
  ^NDBuffer [t reorder-indexes]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/transpose t reorder-indexes))


(defn broadcast
  "Broadcase an element into a new (larger) shape.  The new shape's dimension
  must be even multiples of the old shape's dimensions.  Elements are repeated."
  ^NDBuffer [t new-shape]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/broadcast t new-shape))

(defn rotate
  "Rotate dimensions.  Offset-vec must have same count as the rank of t.  Elements of
  that dimension are rotated by the amount specified in the offset vector with 0
  indicating no rotation."
  ^NDBuffer [t offset-vec]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/rotate t offset-vec))

(defn slice
  "Slice off Y leftmost dimensions returning a reader of objects.
  If all dimensions are sliced of then the reader reads actual elements,
  else it reads subrect tensors."
  ^List [t n-dims]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/slice t n-dims false))

(defn slice-right
  "Slice off Y rightmost dimensions returning a reader of objects.
  If all dimensions are sliced of then the reader reads actual elements,
  else it reads subrect tensors."
  ^List  [t n-dims]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/slice t n-dims true))

(defn mget
  "Get an item from an ND object.  If fewer dimensions are
  specified than exist then the return value is a new tensor as a select operation is
  performed."
  ([t x]
   (check-ns 'tech.v3.tensor)
   (if (instance? NDBuffer t)
     (.ndReadObject ^NDBuffer t x)
     (mget t [x])))
  ([t x y]
   (if (instance? NDBuffer t)
     (.ndReadObject ^NDBuffer t x y)
     (dtype-proto/mget t [x y])))
  ([t x y z]
   (if (instance? NDBuffer t)
     (.ndReadObject ^NDBuffer t x y z)
     (dtype-proto/mget t [x y z])))
  ([t x y z & args]
   (dtype-proto/mget t (concat [x y z] args))))

(defn mset!
  "Set value(s) on an ND object.  If fewer indexes are provided than dimension then a
  tensor assignment is done and value is expected to be the same shape as the subrect
  of the tensor as indexed by the provided dimensions.  Returns t."
  ([t x value]
   (check-ns 'tech.v3.tensor)
   (if (instance? NDBuffer t)
     (.ndWriteObject ^NDBuffer t x value)
     (dtype-proto/mset! t [x] value))
   t)
  ([t x y value]
   (if (instance? NDBuffer t)
     (.ndWriteObject ^NDBuffer t x y value)
     (dtype-proto/mset! t [x y] value))
   t)
  ([t x y z value]
   (if (instance? NDBuffer t)
     (.ndWriteObject ^NDBuffer t x y z value)
     (dtype-proto/mset! t [x y z] value))
   t)
  ([t x y z w & args]
   (let [value (last args)]
     (dtype-proto/mset! t (concat [x y z w] (butlast args)) value)
     t)))
