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
  (:import [tech.v3.datatype Buffer
            ObjectBuffer ElemwiseDatatype ObjectReader NDBuffer]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [clojure.lang IPersistentCollection]
           [java.util RandomAccess List
            Spliterator Spliterator$OfDouble Spliterator$OfLong
            Spliterator$OfInt]
           [java.util.stream Stream DoubleStream LongStream IntStream]))


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
  to the new values upon at read time."
  [item new-dtype]
  (when-not (nil? item) (dtype-proto/elemwise-cast item new-dtype)))


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
  ^Buffer [item]
  (cond
    (nil? item) item
    (and (instance? Buffer item)
         (.allowsRead ^Buffer item))
    item
    :else
    (when (dtype-proto/convertible-to-reader? item)
      (dtype-proto/->reader item))))


(defn ->reader
  "If this object has a read-only or read-write conversion to a buffer
  object return the buffer object.  Else throw an exception."
  ^Buffer [item]
  (if-let [io (as-reader item)]
    io
    (errors/throwf "Item type %s is not convertible to primitive reader"
                   (type item))))


(defn reader?
  "True if this item is convertible to a read-only or read-write Buffer
  interface."
  [item]
  (when item (dtype-proto/convertible-to-reader? item)))


(defn ensure-iterable
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
      (instance? Stream item)))


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
           (.allowsWrite item))
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
  (datatype [item] {:container-type :random-access
                    :elemwise-datatype :object})
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


;;Datatype library Object defaults.  Here lie dragons.
(extend-type Object
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
             (let [src-rdr (->reader item)]
               (reify ObjectReader
                 (elemwiseDatatype [rdr] new-dtype)
                 (lsize [rdr] (.lsize src-rdr))
                 (readObject [rdr idx] (cast-fn (.readObject src-rdr idx))))))
           item)))))
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
  be :all."
  ^NDBuffer [t & new-shape]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/select t new-shape))


(defn transpose
  "In-place transpose an n-d object into a new shape.  Returns a tensor."
  ^NDBuffer [t new-shape]
  (check-ns 'tech.v3.tensor)
  (dtype-proto/transpose t new-shape))


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
