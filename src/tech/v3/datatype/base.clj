(ns tech.v3.datatype.base
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer])
  (:import [tech.v3.datatype PrimitiveReader PrimitiveWriter
            ObjectReader ObjectWriter]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [java.util RandomAccess List]))


(defn elemwise-datatype
  [item]
  (when item (dtype-proto/elemwise-datatype item)))


(defn ecount
  ^long [item]
  (if-not item
    0
    (dtype-proto/ecount item)))


(defn ->reader
  ^PrimitiveReader [item]
  (when-not item (throw (Exception. "Cannot convert nil to reader")))
  (if (instance? PrimitiveReader item)
    item
    (when (dtype-proto/convertible-to-reader? item)
      (dtype-proto/->reader item {}))))


(defn ->writer
  ^PrimitiveWriter [item]
  (when-not item (throw (Exception. "Cannot convert nil to writer")))
  (if (instance? PrimitiveWriter item)
    item
    (when (dtype-proto/convertible-to-writer? item)
      (dtype-proto/->writer item {}))))


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
       (throw (Exception. (format "Offset %d + length (%d) out of range of item length %d"
                                  offset length n-elems))))
     (dtype-proto/sub-buffer item offset length)))
  ([item ^long offset]
   (let [n-elems (ecount item)]
     (sub-buffer item offset (- n-elems offset)))))


(defn- random-access->io
  [^List item]
  (reify
    ObjectReader
    (elemwiseDatatype [rdr] :object)
    (lsize [rdr] (long (.size item)))
    (read [rdr idx]
      (.get item idx))
    ObjectWriter
    (write [wtr idx value]
      (.set item idx value))))


(extend-type RandomAccess
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item options]
    (random-access->io item))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item options]
    (random-access->io item)))


(extend-type PrimitiveReader
  dtype-proto/PToReader
  (convertible-to-reader? [buf] true)
  (->reader [buf options] buf))


(extend-type PrimitiveWriter
  dtype-proto/PToWriter
  (convertible-to-writer? [buf] true)
  (->writer [buf options] buf))


;;Datatype library Object defaults.  Here lie dragons.
(extend-type Object
  dtype-proto/PToReader
  (convertible-to-reader? [buf] (.isArray (.getClass ^Object buf)))
  (->reader [buf options]
    (dtype-proto/->reader
     (array-buffer/array-buffer buf (elemwise-datatype buf))
     options))
  dtype-proto/PToWriter
  (convertible-to-reader? [buf] (.isArray (.getClass ^Object buf)))
  (->writer [buf options]
    (dtype-proto/->writer
     (array-buffer/array-buffer buf (elemwise-datatype buf))
     options))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [buf] false)
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [buf] false))
