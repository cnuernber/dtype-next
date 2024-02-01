(ns tech.v3.datatype.copy-make-container
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy :as dtype-copy]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.const-reader :as const-reader]
            [tech.v3.datatype.reductions :as reductions]
            [tech.v3.datatype.argtypes :as argtypes]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype Buffer]
           [org.apache.commons.math3.exception NotANumberException]
           [ham_fisted IMutList]))


(defn copy!
  "Mutably copy values from a src container into a destination container.
  Returns the destination container."
  ([src dst options]
   (dtype-copy/copy! src dst)
   dst)
  ([src dst]
   (dtype-copy/copy! src dst)
   dst))


(defmethod dtype-proto/make-container :jvm-heap
  [_container-type datatype options elem-seq-or-count]
  (errors/when-not-error
   elem-seq-or-count
   "nil elem-seq-or-count passed into make-container")
  (if (#{:scalar :iterable} (argtypes/arg-type elem-seq-or-count))
    (array-buffer/array-sub-list datatype elem-seq-or-count)
    (let [rdr (dtype-base/->reader elem-seq-or-count datatype)]
      (case datatype
        :float64 (array-buffer/array-buffer (hamf/double-array rdr))
        :float32 (array-buffer/array-buffer (hamf/float-array rdr))
        :int64 (array-buffer/array-buffer (hamf/long-array rdr))
        :int32 (array-buffer/array-buffer (hamf/int-array rdr))
        (let [data (array-buffer/array-sub-list datatype (.lsize rdr))]
          (copy! elem-seq-or-count data options)
          data)))))


(defmethod dtype-proto/make-container :java-array
  [_container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :jvm-heap datatype options elem-seq-or-count))


;;Backwards compatibility
(defmethod dtype-proto/make-container :typed-buffer
  [_container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :jvm-heap datatype options elem-seq-or-count))


(defmethod dtype-proto/make-container :native-heap
  [_container-type datatype options elem-seq-or-count]
  (let [n-elems (long (if (number? elem-seq-or-count)
                        elem-seq-or-count
                        (dtype-base/ecount elem-seq-or-count)))
        n-bytes (* n-elems (casting/numeric-byte-width datatype))
        native-buf (-> (native-buffer/malloc n-bytes options)
                       (native-buffer/set-native-datatype datatype))]
    (when-not (number? elem-seq-or-count)
      (copy! elem-seq-or-count native-buf options))
    native-buf))



(defmethod dtype-proto/make-container :native-heap-LE
  [_container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :native-heap datatype
                              (assoc options :endianness :little-endian)
                              elem-seq-or-count))


(defmethod dtype-proto/make-container :native-heap-BE
  [_container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :native-heap datatype
                              (assoc options :endianness :big-endian)
                              elem-seq-or-count))


(defmethod dtype-proto/make-container :native-buffer
  [_container-type datatype _options elem-seq-or-count]
  (dtype-proto/make-container :native-heap datatype
                              nil
                              elem-seq-or-count))


(defn make-container
  "Make a container of a given datatype.  Options are container specific
  and in general unused.  Values will be copied into given container using
  the most efficient pathway possible."
  ([container-type datatype options elem-seq-or-count]
   (dtype-proto/make-container container-type datatype options elem-seq-or-count))
  ([container-type datatype elem-seq-or-count]
   (dtype-proto/make-container container-type datatype {} elem-seq-or-count))
  ([datatype elem-seq-or-count]
   (dtype-proto/make-container :jvm-heap datatype {} elem-seq-or-count))
  ([elem-seq-or-count]
   (when elem-seq-or-count
     (when (number? elem-seq-or-count)
       (throw (Exception. "Must provide existing container of items")))
     (dtype-proto/make-container :jvm-heap
                                 (dtype-base/elemwise-datatype elem-seq-or-count)
                                 {}
                                 elem-seq-or-count))))


(defn- apply-nan-strat
  [datatype nan-strategy data]
  (if (or (identical? datatype :float32)
          (identical? datatype :float64))
    (case (or nan-strategy :keep)
      :keep data
      :remove (lznc/filter (fn [^double v]
                             (not (Double/isNaN v)))
                           data)
      :exception (lznc/map (fn ^double [^double v]
                             (when-not (Double/isNaN v)
                               (throw (NotANumberException.)))
                             v)
                           data))
    data))


(defn ->array-buffer
  "Perform a NaN-aware conversion into an array buffer.  Default
  nan-strategy is :keep.


  Options:

  * `:nan-strategy` - Nan strategies can be: [:keep :remove :exception].  `:remove` can
  result in a shorter array buffer than the input data.  Default is `:keep`."
  (^ArrayBuffer [datatype {:keys [nan-strategy]
                           :or {nan-strategy :keep}
                           :as _options} item]
   (let [item (apply-nan-strat datatype nan-strategy item)
         data (if (and (= datatype (dtype-base/elemwise-datatype item))
                       (dtype-base/as-array-buffer item))
                (dtype-base/as-array-buffer item)
                (make-container datatype item))]
     (dtype-proto/->array-buffer data)))
  (^ArrayBuffer [datatype item]
   (->array-buffer datatype nil item))
  (^ArrayBuffer [item]
   (->array-buffer (packing/unpack-datatype (dtype-base/elemwise-datatype item))
                   nil item)))


(defn ->array
  "Perform a NaN-aware conversion into an array.  Default
  nan-strategy is :keep.
  Nan strategies can be: [:keep :remove :exception]"
  ([datatype {:keys [nan-strategy]
              :or {nan-strategy :keep} :as options}
    item]
   (errors/when-not-error
    item
    "nil value passed into ->array")
   (let [item (apply-nan-strat datatype nan-strategy item)
         abuf (dtype-base/as-array-buffer item)]
     (if (and abuf (identical? (.-dtype abuf) (casting/datatype->host-datatype datatype)))
       (if (and (== (.-offset abuf) 0)
                (== (.-n-elems abuf)
                    (dtype-base/ecount (.-ary-data abuf))))
         (.-ary-data abuf)
         (array-buffer/copy-of abuf))
       (-> (make-container (casting/datatype->safe-host-type datatype) (if abuf abuf item))
           (dtype-base/as-array-buffer)
           (->array)))))
  ([datatype item]
   (->array datatype nil item))
  ([item]
   (->array (packing/unpack-datatype (dtype-base/elemwise-datatype item))
            nil item)))

(defn ->byte-array
  "Efficiently convert nearly anything into a byte array."
  ^bytes [data]
  (if (instance? (Class/forName "[B") data)
    data
    (->array :int8 nil data)))

(defn ->short-array
  "Efficiently convert nearly anything into a short array."
  ^shorts [data]
  (if (instance? (Class/forName "[S") data)
    data
    (->array :int16 nil data)))

(defn ->char-array
  "Efficiently convert nearly anything into a char array."
  ^chars [data]
  (if (instance? (Class/forName "[C") data)
    data
    (->array :char nil data)))


(defn ->int-array
  "Efficiently convert nearly anything into a int array."
  ^ints [data]
  (if (instance? (Class/forName "[I") data)
    data
    (->array :int32 nil data)))

(defn ->long-array
  "Efficiently convert nearly anything into a long array."
  ^longs [data]
  (if (instance? (Class/forName "[J") data)
    data
    (->array :int64 nil data)))

(defn ->float-array
    "Nan-aware conversion to double array.
  See documentation for ->array-buffer.  Returns a double array.
  options -
   * nan-strategy - :keep (default) :remove :exception"
  (^floats [options data]
   (if (and (identical? :keep (get options :nan-strategy :keep))
            (instance? (Class/forName "[F") data))
     data
     (->array :float32 options data)))
  (^floats [data]
   (->float-array nil data)))


(defn ->double-array
  "Nan-aware conversion to double array.
  See documentation for ->array-buffer.  Returns a double array.
  options -
   * nan-strategy - :keep (default) :remove :exception"
  (^doubles [options data]
   (if (and (identical? :keep (get options :nan-strategy :keep))
            (instance? (Class/forName "[D") data))
     data
     (->array :float64 options data)))
  (^doubles [data]
   (->double-array nil data)))


(defn ensure-reader
  "Ensure item is randomly addressable.  This may copy the data into a randomly
  accessible container."
  (^Buffer [item n-const-elems]
   (let [argtype (argtypes/arg-type item)]
     (cond
       (= argtype :scalar)
       (const-reader/const-reader item n-const-elems)
       (= argtype :iterable)
       (-> (make-container :list (dtype-base/operational-elemwise-datatype item) {}
                           item)
           (dtype-base/->reader))
       :else
       (dtype-base/->reader item (dtype-base/operational-elemwise-datatype item)))))
  (^Buffer [item]
   (ensure-reader item Long/MAX_VALUE)))
