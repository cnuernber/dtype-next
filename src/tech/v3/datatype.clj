(ns tech.v3.datatype
  "Base namespace for container creation and elementwise access of data"
  (:require [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.array-buffer]
            [tech.v3.datatype.native-buffer]
            [tech.v3.datatype.list]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-copy-make-container]
            [tech.v3.datatype.clj-range]
            [tech.v3.datatype.functional]
            [tech.v3.datatype.copy-raw-to-item]
            [tech.v3.datatype.primitive]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.nio-buffer]
            [tech.v3.datatype.io-indexed-buffer :as io-idx-buf]
            [tech.v3.datatype.io-concat-buffer]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.emap :as emap]
            [tech.v3.datatype.export-symbols :refer [export-symbols]])
  (:import [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype ListPersistentVector BooleanReader
            LongReader DoubleReader ObjectReader]
           [org.roaringbitmap RoaringBitmap]
           [java.util List])
  (:refer-clojure :exclude [cast]))


(export-symbols tech.v3.datatype.casting
                cast
                unchecked-cast)


(export-symbols tech.v3.datatype.dispatch
                vectorized-dispatch-1
                vectorized-dispatch-2)


(export-symbols tech.v3.datatype.argtypes
                arg-type
                reader-like?)


(export-symbols tech.v3.datatype.base
                elemwise-datatype
                elemwise-cast
                ecount
                shape
                as-buffer
                ->buffer
                as-reader
                ->reader
                reader?
                ensure-iterable
                as-writer
                ->writer
                writer?
                as-array-buffer
                as-native-buffer
                ->native-buffer
                as-concrete-buffer
                sub-buffer
                get-value
                set-value!
                set-constant!)


(export-symbols tech.v3.datatype.emap
                emap)


(export-symbols tech.v3.datatype.argops
                ensure-reader)


(defn get-datatype
  "Legacy method, returns elemwise-datatype"
  [item]
  (elemwise-datatype item))


(defn set-datatype
  "Legacy method.  Performs elemwise-cast."
  [item new-dtype]
  (dtype-proto/elemwise-cast item new-dtype))


(defn clone
  "Clone an object.  Can clone anything convertible to a reader."
  [item]
  (when item
    (dtype-proto/clone item)))


(defmacro make-reader
  "Make a reader.  Datatype must be a compile time visible object.
  read-op has 'idx' in scope which is the index to read from.  Returns a
  reader of the appropriate type for the passed in datatype.  Results are unchecked
  casted to the appropriate datatype.  It is up to *you* to ensure this is the result
  you want or throw an exception.

  This function creates a compile-time object that can allow maximum performance.  For
  most use cases this is probably overkill and 'emap' is a more ideal pathway that
  matches Clojure constructs a bit closer.

  reader-datatype must be a compile time constant but advertised datatype need not be.

user> (dtype/make-reader :float32 5 idx)
[0.0 1.0 2.0 3.0 4.0]
user> (dtype/make-reader :boolean 5 idx)
[true true true true true]
user> (dtype/make-reader :boolean 5 (== idx 0))
[true false false false false]
user> (dtype/make-reader :float32 5 (* idx 2))
 [0.0 2.0 4.0 6.0 8.0]"
  ([datatype n-elems read-op]
   `(make-reader ~datatype ~datatype ~n-elems ~read-op))
  ([reader-datatype advertised-datatype n-elems read-op]
   `(do
      (casting/ensure-valid-datatype ~advertised-datatype)
      (casting/ensure-valid-datatype ~reader-datatype)
      (let [~'n-elems (long ~n-elems)]
        ~(cond
           (= :boolean reader-datatype)
           `(reify BooleanReader
              (elemwiseDatatype [rdr#] ~advertised-datatype)
              (lsize [rdr#] ~'n-elems)
              (readBoolean [rdr# ~'idx]
                (casting/datatype->unchecked-cast-fn
                 :unknown :boolean ~read-op)))
           (casting/integer-type? reader-datatype)
           `(reify LongReader
              (elemwiseDatatype [rdr#] ~advertised-datatype)
              (lsize [rdr#] ~'n-elems)
              (readLong [rdr# ~'idx] (unchecked-long ~read-op)))
           (casting/float-type? reader-datatype)
           `(reify DoubleReader
              (elemwiseDatatype [rdr#] ~advertised-datatype)
              (lsize [rdr#] ~'n-elems)
              (readDouble [rdr# ~'idx] (unchecked-double ~read-op)))
           :else
           `(reify ObjectReader
              (elemwiseDatatype [rdr#] ~advertised-datatype)
              (lsize [rdr#] ~'n-elems)
              (readObject [rdr# ~'idx] ~read-op)))))))


(export-symbols tech.v3.datatype.io-indexed-buffer
                indexed-buffer)

(export-symbols tech.v3.datatype.const-reader
                const-reader)

(export-symbols tech.v3.datatype.io-concat-buffer
                concat-buffers)


(export-symbols tech.v3.datatype.copy-make-container
                make-container
                copy!
                ->array-buffer
                ->array
                ->byte-array
                ->short-array
                ->char-array
                ->int-array
                ->long-array
                ->float-array
                ->double-array)


(defn ->array-copy
  "Create a new array that contains the data from the original container.
  Returns a java array."
  [item]
  (.ary-data ^ArrayBuffer (make-container (elemwise-datatype item) item)))


(defn copy-raw->item!
  "Copy raw data into a buffer.  Data may be a sequence of numbers or a sequence
  of containers.  Data will be coalesced into the buffer.  Returns a tuple of:
  [buffer final-offset]."
  ([raw-data buffer options]
   (dtype-proto/copy-raw->item! raw-data buffer 0 options))
  ([raw-data buffer]
   (dtype-proto/copy-raw->item! raw-data buffer 0 {})))


(defn coalesce!
  "Coalesce data from a raw sequence of things into a contiguous buffer.
  Returns the buffer; uses the copy-raw->item! pathway."
  [buffer raw-data]
  (first (copy-raw->item! raw-data buffer)))


(defn as-roaring-bitmap
  ^RoaringBitmap [item]
  (dtype-proto/as-roaring-bitmap item))


(defn ->vector
  "Copy a thing into a persistent vector."
  [item]
  (if-let [rdr (as-reader item)]
    (vec rdr)
    (vec item)))


(defn as-persistent-vector
  "Return a reader wrapped in APersistentVector meaning you can use the reader
  as, for instance, keys in a map.  Not recommended far large readers although
  the data is shared."
  [item]
  (ListPersistentVector. (->reader item)))


(defn as-nd-buffer-descriptor
  "If this item is convertible to a buffer descriptor, convert it.  Else
  return nil.

  Buffer descriptors are a ND description of data.  For example, a native
  3x3 tensor has a buffer description like thus:

  ```clojure
  {:ptr 140330005614816
   :datatype :float64
   :endianness :little-endian
   :shape [3 3]
   :strides [24 8]}
  ```

  This design allows zero-copy transfer between neanderthal, numpy, tvm, etc."
  [src-item]
  (when (dtype-proto/convertible-to-nd-buffer-desc? src-item)
    (dtype-proto/->nd-buffer-descriptor src-item)))
