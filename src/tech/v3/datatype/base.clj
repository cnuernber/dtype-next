(ns tech.v3.datatype.base
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.io-sub-buffer :as io-sub-buf]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.parallel.for :as parallel-for])
  (:import [tech.v3.datatype PrimitiveReader PrimitiveWriter PrimitiveIO
            ObjectIO]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [java.util RandomAccess List]))


(defn elemwise-datatype
  [item]
  (when-not (nil? item) (dtype-proto/elemwise-datatype item)))


(defn ecount
  ^long [item]
  (if-not item
    0
    (dtype-proto/ecount item)))


(defn ->io
  ^PrimitiveIO [item]
  (when-not item (throw (Exception. "Cannot convert nil to reader")))
  (if (instance? PrimitiveIO item)
    item
    (when (dtype-proto/convertible-to-primitive-io? item)
      (dtype-proto/->primitive-io item))))


(defn ->reader
  (^PrimitiveReader [item options]
   (when-not item (throw (Exception. "Cannot convert nil to reader")))
   (if (instance? PrimitiveReader item)
     item
     (when (dtype-proto/convertible-to-reader? item)
       (dtype-proto/->reader item options ))))
  (^PrimitiveReader [item]
   (->reader item {})))


(defn ->writer
  (^PrimitiveWriter [item options]
   (when-not item (throw (Exception. "Cannot convert nil to writer")))
   (if (instance? PrimitiveWriter item)
     item
     (when (dtype-proto/convertible-to-writer? item)
       (dtype-proto/->writer item options))))
  (^PrimitiveWriter [item]
   (->writer item {})))


(defn ->array-buffer
  ^ArrayBuffer
  [item]
  (if (instance? ArrayBuffer item)
    item
    (when (dtype-proto/convertible-to-array-buffer? item)
      (dtype-proto/->array-buffer item))))


(defn ->native-buffer
  ^NativeBuffer
  [item]
  (if (instance? NativeBuffer item)
    item
    (when (dtype-proto/convertible-to-native-buffer? item)
      (dtype-proto/->native-buffer item))))


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
    (read [rdr idx]
      (.get item idx))
    (write [wtr idx value]
      (.set item idx value))))


(extend-type RandomAccess
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [item] true)
  (->io [item] (random-access->io item))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (random-access->io item))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (random-access->io item)))


(defn- inner-buffer-sub-buffer
  [buf ^long offset ^Long len]
  (when-let [data-buf (or (->array-buffer buf)
                          (->native-buffer buf))]
    (with-meta
      (sub-buffer data-buf offset len)
      (meta buf))))


(extend-type PrimitiveIO
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [buf] true)
  (->io [item] item)
  dtype-proto/PToReader
  (convertible-to-reader? [buf] true)
  (->reader [buf options] buf)
  dtype-proto/PToWriter
  (convertible-to-writer? [buf] true)
  (->writer [buf options] buf)
  dtype-proto/PClone
  (clone [buf]
    (dtype-proto/make-container :jvm-heap (elemwise-datatype buf) buf {}))
  dtype-proto/PSetConstant
  (set-constant! [buf offset value elem-count]
    (let [value (casting/cast value (elemwise-datatype buf))]
      (parallel-for/parallel-for
       idx (.lsize buf)
       (.writeObject buf idx value)))
    buf))


;;Datatype library Object defaults.  Here lie dragons.
(extend-type Object
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
    (if-let [buf-data (or (->array-buffer buf)
                            (->native-buffer buf))]
      (dtype-proto/->primitive-io buf-data)
      (when (.isArray (.getClass ^Object buf))
        (dtype-proto/->primitive-io (array-buffer/array-buffer buf)))))
  dtype-proto/PToReader
  (convertible-to-reader? [buf]
    (dtype-proto/convertible-to-primitive-io? buf))
  (->reader [buf options]
    (dtype-proto/->primitive-io buf))
  dtype-proto/PToWriter
  (convertible-to-reader? [buf]
    (dtype-proto/convertible-to-primitive-io? buf))
  (->writer [buf options]
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
    (if-let [buf-data (or (->array-buffer buf)
                          (->native-buffer buf))]
      ;;highest performance
      (dtype-proto/set-constant! buf-data offset value element-count)
      (if-let [writer (->writer buf {})]
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
    (if-let [buf-data (or (->array-buffer buf)
                          (->native-buffer buf))]
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
  (convertible-to-array-buffer? [buf] false))
