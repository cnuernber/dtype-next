(ns tech.v3.datatype.copy-make-container
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy :as dtype-copy]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.reductions :as reductions])
  (:import [tech.v3.datatype.array_buffer ArrayBuffer]))


(defn copy!
  "Mutably copy values from a src container into a destination container.
  Returns the destination container."
  ([src dst options]
   (dtype-copy/copy! src dst (:unchecked? options))
   dst)
  ([src dst]
   (dtype-copy/copy! src dst true)
   dst))


(defmethod dtype-proto/make-container :jvm-heap
  [container-type datatype options elem-seq-or-count]
  (if (or (number? elem-seq-or-count)
          (dtype-base/as-reader elem-seq-or-count))
    (let [n-elems (long (if (number? elem-seq-or-count)
                          elem-seq-or-count
                          (dtype-base/ecount elem-seq-or-count)))
          ary-data
          (case (casting/host-flatten datatype)
            :boolean (boolean-array n-elems)
            :int8 (byte-array n-elems)
            :int16 (short-array n-elems)
            :char (char-array n-elems)
            :int32 (int-array n-elems)
            :int64 (long-array n-elems)
            :float32 (float-array n-elems)
            :float64 (double-array n-elems)
            (make-array (casting/datatype->object-class datatype) n-elems))
          ary-buf (array-buffer/array-buffer ary-data datatype)]
      (when-not (number? elem-seq-or-count)
        (copy! elem-seq-or-count ary-buf options))
      ary-buf)
    (-> (dtype-proto/make-container :list datatype options elem-seq-or-count)
        (dtype-base/as-array-buffer))))


(defmethod dtype-proto/make-container :java-array
  [container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :jvm-heap datatype options elem-seq-or-count))


;;Backwards compatibility
(defmethod dtype-proto/make-container :typed-buffer
  [container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :jvm-heap datatype options elem-seq-or-count))


(defmethod dtype-proto/make-container :native-heap
  [container-type datatype options elem-seq-or-count]
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
  [container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :native-heap datatype
                              (assoc options :endianness :little-endian)
                              elem-seq-or-count))


(defmethod dtype-proto/make-container :native-heap-BE
  [container-type datatype options elem-seq-or-count]
  (dtype-proto/make-container :native-heap datatype
                              (assoc options :endianness :big-endian)
                              elem-seq-or-count))


(defmethod dtype-proto/make-container :native-buffer
  [container-type datatype options elem-seq-or-count]
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


(defn ->array-buffer
  "Perform a NaN-aware conversion into an array buffer.  Default
  nan-strategy is :remove which forces a pass over float datatypes
  in order to remove nan data.


  Nan strategies can be: [:keep :remove :exception]"
  (^ArrayBuffer [datatype {:keys [nan-strategy]} item]
   (let [nan-strategy (if (or (= datatype :float32)
                              (= datatype :float64))
                        nan-strategy
                        :keep)]
     (if (= nan-strategy :keep)
       (if (and (= datatype (dtype-base/elemwise-datatype item))
                (dtype-base/as-array-buffer item))
         (dtype-base/as-array-buffer item)
         (make-container datatype item))
       (->> (reductions/reader->double-spliterator item nan-strategy)
            (make-container datatype)))))
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
   (let [options (assoc options :nan-strategy nan-strategy)
         array-buffer (->array-buffer datatype options item)]
     (if (and (== (.offset array-buffer) 0)
              (== (.n-elems array-buffer)
                  (dtype-base/ecount (.ary-data array-buffer))))
       (.ary-data array-buffer)
       (.ary-data ^ArrayBuffer (make-container :jvm-heap datatype array-buffer)))))
  ([datatype item]
   (->array datatype nil item))
  (^ArrayBuffer [item]
   (->array (packing/unpack-datatype (dtype-base/elemwise-datatype item))
            nil item)))

(defn ->byte-array
  "Efficiently convert nearly anything into a byte array."
  ^bytes [data]
  (->array :int8 nil data))

(defn ->short-array
  "Efficiently convert nearly anything into a short array."
  ^shorts [data]
  (->array :int16 nil data))

(defn ->char-array
  "Efficiently convert nearly anything into a char array."
  ^chars [data]
  (->array :char nil data))


(defn ->int-array
  "Efficiently convert nearly anything into a int array."
  ^ints [data]
  (->array :int32 nil data))

(defn ->long-array
  "Efficiently convert nearly anything into a long array."
  ^longs [data]
  (->array :int64 nil data))

(defn ->float-array
    "Nan-aware conversion to double array.
  See documentation for ->array-buffer.  Returns a double array.
  options -
   * nan-strategy - :keep (default) :remove :exception"
  (^doubles [options data]
   (->array :float32 options data))
  (^doubles [data]
   (->array :float32 data)))


(defn ->double-array
  "Nan-aware conversion to double array.
  See documentation for ->array-buffer.  Returns a double array.
  options -
   * nan-strategy - :keep (default) :remove :exception"
  (^doubles [options data]
   (->array :float64 options data))
  (^doubles [data]
   (->array :float64 data)))
