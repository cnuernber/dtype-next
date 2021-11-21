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
            [tech.v3.datatype.io-indexed-buffer :as io-idx-buf]
            [tech.v3.datatype.io-concat-buffer]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.monotonic-range :as dt-range]
            [tech.v3.datatype.emap :as emap]
            ;;import in clone for jvm maps
            [tech.v3.datatype.jvm-map]
            [tech.v3.datatype.export-symbols :refer [export-symbols]])
  (:import [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype ListPersistentVector BooleanReader
            LongReader DoubleReader ObjectReader PrimitiveList
            Buffer]
           [org.roaringbitmap RoaringBitmap]
           [java.util List])
  (:refer-clojure :exclude [cast reverse]))


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
                datatype
                elemwise-cast
                ecount
                shape
                as-buffer
                ->buffer
                as-reader
                ->reader
                reader?
                ->iterable
                iterable?
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


(defn reverse
  "Reverse an sequence, range or reader.
  * If range, returns a new range.
  * If sequence, uses clojure.core/reverse
  * If reader, returns a new reader that performs an in-place reverse."
  [item]
  (if (dtype-proto/convertible-to-range? item)
    (let [rng (dtype-proto/->range item nil)
          rstart (dtype-proto/range-start rng)
          rc (ecount rng)
          rinc (dtype-proto/range-increment rng)
          rend (+ rstart (* rinc rc))]
      ;;unchecked math intentional
      (dt-range/make-range (- rend rinc) (- rstart rinc) (- rinc)))
    (let [rc (ecount item)
          drc (dec rc)]
      (dispatch/vectorized-dispatch-1
       (constantly item)
       (constantly (clojure.core/reverse item))
       (fn [res-dt ^Buffer rdr]
         (case (casting/simple-operation-space res-dt)
           :int64 (make-reader res-dt rc (.readLong rdr (- drc idx)))
           :float64 (make-reader res-dt rc (.readDouble rdr (- drc idx)))
           :boolean (make-reader res-dt rc (.readBoolean rdr (- drc idx)))
           (make-reader res-dt rc (.readObject rdr (- drc idx)))))
       item))))


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


(defn make-list
  "Make an instance of a tech.v3.datatype.PrimitiveList.  These have typed add*
  methods, implement tech.v3.datatype.Buffer, and a guaranteed in-place transformation
  to a concrete buffer (defaults to an array buffer)."
  (^PrimitiveList [datatype n-elems-or-data]
   (make-container :list datatype n-elems-or-data))
  (^PrimitiveList [datatype]
   (make-list datatype 0)))


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


(defn coalesce-blocks!
  "Copy a sequence of blocks of countable things into a larger
  countable thing."
  [dst src-seq]
  (reduce (fn [offset src-item]
            (let [n-elems (ecount src-item)]
              (copy! src-item (sub-buffer dst offset n-elems))
              (+ (long offset) n-elems)))
          0
          src-seq)
  dst)


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


(defn ensure-serializeable
  "Ensure this is an efficiently serializeable datatype object.
  For nippy support across buffers and tensors require `tech.v3.datatype.nippy`."
  [item]
  (when item
    (case (arg-type item)
      :scalar item
      :iterable (->array-buffer item)
      :reader (->array-buffer item)
      :tensor (if-let [retval (dtype-proto/as-tensor item)]
                retval
                (errors/throwf "Unable create create tensor for nd object type: %s"
                               (type item))))))
