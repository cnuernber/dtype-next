(ns tech.v3.datatype.base
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.io-sub-buffer :as io-sub-buf]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.parallel.for :as parallel-for])
  (:import [tech.v3.datatype PrimitiveReader PrimitiveWriter PrimitiveIO
            ObjectIO ElemwiseDatatype ObjectReader]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [clojure.lang IPersistentCollection]
           [java.util RandomAccess List]
           [java.util.stream Stream]))


(defn elemwise-datatype
  [item]
  (when-not (nil? item) (dtype-proto/elemwise-datatype item)))


(defn elemwise-cast
  [item new-dtype]
  (when-not (nil? item) (dtype-proto/elemwise-cast item new-dtype)))


(defn ecount
  ^long [item]
  (if-not item
    0
    (dtype-proto/ecount item)))


(defn shape
  [item]
  (if-not item
    nil
    (dtype-proto/shape item)))


(defn as-io
  ^PrimitiveIO [item]
  (when-not item (throw (Exception. "Cannot convert nil to reader")))
  (if (instance? PrimitiveIO item)
    item
    (when (dtype-proto/convertible-to-primitive-io? item)
      (dtype-proto/->primitive-io item))))


(defn ->io
  ^PrimitiveIO [item]
  (if-let [io (as-io item)]
    io
    (errors/throwf "Item type %s is not convertible to primitive io"
                   (type item))))


(defn as-reader
  ^PrimitiveReader [item]
  (cond
    (nil? item) item
    (instance? PrimitiveReader item)
    item
    :else
    (when (dtype-proto/convertible-to-reader? item)
      (dtype-proto/->reader item))))


(defn ->reader
  ^PrimitiveReader [item]
  (if-let [io (as-reader item)]
    io
    (errors/throwf "Item type %s is not convertible to primitive reader"
                   (type item))))


(defn ensure-reader
  "Ensure item is randomly addressable"
  ^PrimitiveReader [item]
  (if-let [rdr (as-reader item)]
    rdr
    (-> (dtype-proto/make-container :list (elemwise-datatype item) {} item)
        (->reader))))


(defn reader?
  [item]
  (when item (dtype-proto/convertible-to-reader? item)))


(defn ensure-iterable
  (^Iterable [item]
   (cond
     (instance? Iterable item)
     item
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
  [item]
  (or (instance? Iterable item)
      (instance? Stream item)))


(defn writer?
  [item]
  (when item
    (dtype-proto/convertible-to-writer? item)))


(defn as-writer
  ^PrimitiveWriter [item]
  (when-not item (throw (Exception. "Cannot convert nil to writer")))
  (if (instance? PrimitiveWriter item)
    item
    (when (dtype-proto/convertible-to-writer? item)
      (dtype-proto/->writer item))))


(defn ->writer
  ^PrimitiveWriter [item]
  (if-let [io (as-writer item)]
    io
    (errors/throwf "Item type %s is not convertible to primitive writer"
                   (type item))))


(defn as-array-buffer
  ^ArrayBuffer
  [item]
  (if (instance? ArrayBuffer item)
    item
    (when (dtype-proto/convertible-to-array-buffer? item)
      (dtype-proto/->array-buffer item))))


(defn ->array-buffer
  ^ArrayBuffer [item]
  (if-let [retval (as-array-buffer item)]
    retval
    (errors/throwf "Item type %s is not convertible to an array buffer"
                   (type item))))


(defn as-native-buffer
  ^NativeBuffer
  [item]
  (if (instance? NativeBuffer item)
    item
    (when (dtype-proto/convertible-to-native-buffer? item)
      (dtype-proto/->native-buffer item))))


(defn ->native-buffer
  ^NativeBuffer [item]
  (if-let [retval (as-native-buffer item)]
    retval
    (errors/throwf "Item type %s is not convertible to an native buffer"
                   (type item))))


(defn as-buffer
  [item]
  (or (as-array-buffer item)
      (as-native-buffer item)))


(defn sub-buffer
  ([item ^long offset ^long length]
   (let [n-elems (ecount item)]
     (when-not (<= (+ offset length) n-elems)
       (throw (Exception. (format
                           "Offset %d + length (%d) out of range of item length %d"
                           offset length n-elems))))
     (dtype-proto/sub-buffer item offset length)))
  ([item ^long offset]
   (let [n-elems (ecount item)]
     (sub-buffer item offset (- n-elems offset)))))


(defn get-value
  [item idx]
  ((->reader item) idx))


(defn set-value!
  [item idx value]
  ((->writer item) idx value))


(defn- random-access->io
  [^List item]
  (reify
    ObjectIO
    (elemwiseDatatype [rdr] :object)
    (lsize [rdr] (long (.size item)))
    (readObject [rdr idx]
      (.get item idx))
    (writeObject [wtr idx value]
      (.set item idx value))))


(extend-type RandomAccess
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [item] true)
  (->primitive-io [item] (random-access->io item))
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


(defn- inner-buffer-sub-buffer
  [buf ^long offset ^Long len]
  (when-let [data-buf (as-buffer buf)]
    (with-meta
      (sub-buffer data-buf offset len)
      (meta buf))))


(extend-type PrimitiveIO
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [buf] true)
  (->io [item] item)
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
  (set-constant! [buf offset value elem-count]
    (let [value (casting/cast value (elemwise-datatype buf))]
      (parallel-for/parallel-for
       idx (.lsize buf)
       (.writeObject buf idx value)))
    buf))


(declare shape)


(defn scalar?
  [item]
  (or (number? item)
      (string? item)
      (and
       (not (when (instance? Class (type item))
              (.isArray ^Class (type item))))
       (not (iterable? item))
       (not (reader? item)))))


;;Datatype library Object defaults.  Here lie dragons.
(extend-type Object
  dtype-proto/PElemwiseCast
  (elemwise-cast [item new-dtype]
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
       item)))
  dtype-proto/PCountable
  (ecount [item] (count item))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [buf] (.isArray (.getClass ^Object buf)))
  (->array-buffer [buf]
    (when-not (.isArray (.getClass ^Object buf))
      (throw (Exception. "Item is not an array: %s" (type buf))))
    (array-buffer/array-buffer buf))
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [buf]
    (or (dtype-proto/convertible-to-array-buffer? buf)
        (dtype-proto/convertible-to-native-buffer? buf)
        (.isArray (.getClass ^Object buf))))
  (->primitive-io [buf]
    (if-let [buf-data (as-buffer buf)]
      (dtype-proto/->primitive-io buf-data)
      (if (.isArray (.getClass ^Object buf))
        (dtype-proto/->primitive-io (array-buffer/array-buffer buf))
        (errors/throwf "Buffer type %s is not convertible to primitive-io"
                       (type buf)))))
  dtype-proto/PToReader
  (convertible-to-reader? [buf]
    (dtype-proto/convertible-to-primitive-io? buf))
  (->reader [buf]
    (dtype-proto/->primitive-io buf))
  dtype-proto/PToWriter
  (convertible-to-writer? [buf]
    (if-not (instance? IPersistentCollection buf)
      (dtype-proto/convertible-to-primitive-io? buf)
      false))
  (->writer [buf]
    (dtype-proto/->primitive-io buf))
  dtype-proto/PBuffer
  (sub-buffer [buf offset len]
    (if-let [data-buf (inner-buffer-sub-buffer buf offset len)]
      data-buf
      (if-let [data-io (->io buf)]
        (io-sub-buf/sub-buffer data-io offset len)
        (throw (Exception. (format
                            "Buffer %s does not implement the sub-buffer protocol"
                            (type buf)))))))
  dtype-proto/PSetConstant
  (set-constant! [buf offset value element-count]
    (if-let [buf-data (as-buffer buf)]
      ;;highest performance
      (dtype-proto/set-constant! buf-data offset value element-count)
      (if-let [writer (->writer buf)]
        (dtype-proto/set-constant! writer offset value element-count)
        (throw
         (Exception.
          (format "Buffer is not convertible to writer, array or native buffer: %s"
                  (type buf))))))
    buf)
  ;;Note the lack of reflection here now.  The standard way is to use reflection to
  ;;find a clone method and this of course breaks graal native.
  dtype-proto/PClone
  (clone [buf]
    (if-let [buf-data (as-buffer buf)]
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
  dtype-proto/PShape
  (shape [item]
    (cond
      (scalar? item)
      nil
      (and (instance? Class (type item))
           (.isArray ^Class (type item)))
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
    (if (sequential? (first item))
      (->> (concat [(.size item)]
                   (dtype-proto/shape (first item)))
           vec)
      [(.size item)])))
