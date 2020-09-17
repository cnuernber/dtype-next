(ns tech.v3.datatype
  (:require [tech.v3.datatype.array-buffer]
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
            [tech.v3.datatype.nio-buffer]
            [tech.v3.datatype.io-indexed-buffer :as io-idx-buf]
            [tech.v3.datatype.export-symbols :refer [export-symbols]])
  (:import [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype ListPersistentVector BooleanReader
            LongReader DoubleReader ObjectReader])
  (:refer-clojure :exclude [cast]))


(export-symbols tech.v3.datatype.casting
                cast
                unchecked-cast)


(export-symbols tech.v3.datatype.dispatch
                typed-map
                vectorized-dispatch-1
                vectorized-dispatch-2)


(export-symbols tech.v3.datatype.base
                elemwise-datatype
                elemwise-cast
                ecount
                shape
                ->io
                as-reader
                ->reader
                reader?
                ensure-reader
                ensure-iterable
                as-writer
                ->writer
                writer?
                as-array-buffer
                ->array-buffer
                as-native-buffer
                ->native-buffer
                sub-buffer
                get-value
                set-value!)


(defn get-datatype
  [item]
  (elemwise-datatype item))


(defn set-datatype
  [item new-dtype]
  (dtype-proto/elemwise-cast item new-dtype))


(defn clone
  [item]
  (when item
    (dtype-proto/clone item)))


(defn reader-as-persistent-vector
  [item]
  (ListPersistentVector. (->reader item)))


(defmacro make-reader
  "Make a reader.  Datatype must be a compile time visible object.
  read-op has 'idx' in scope which is the index to read from.  Returns a
  reader of the appropriate type for the passed in datatype.  Results are unchecked
  casted to the appropriate datatype.  It is up to *you* to ensure this is the result
  you want or throw an exception.

  reader-datatype must be a compile time constant but advertised datatype need not be.

user> (dtype/make-reader :float32 5 idx)
[0.0 1.0 2.0 3.0 4.0]
user> (dtype/make-reader :boolean 5 idx)
[true true true true true]
user> (dtype/make-reader :boolean 5 (== idx 0))
[true false false false false]
user> (dtype/make-reader :float32 5 (* idx 2))
 [0.0 2.0 4.0 6.0 8.0]
user> (dtype/make-reader :any-datatype-you-wish 5 (* idx 2))
[0 2 4 6 8]
user> (dtype/get-datatype *1)
:any-datatype-you-wish
user> (dtype/make-reader [:a :b] 5 (* idx 2))
[0 2 4 6 8]
user> (dtype/get-datatype *1)
[:a :b]"
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

(export-symbols tech.v3.datatype.copy-make-container
                make-container
                copy!)


(defn ->array-copy
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


(defn ->vector
  "Convert a datatype thing to a vector"
  [item]
  (if-let [rdr (as-reader item)]
    (vec rdr)
    (vec item)))
