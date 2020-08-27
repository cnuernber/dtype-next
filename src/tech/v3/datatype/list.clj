(ns tech.v3.datatype.list
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy :as dtype-copy]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.parallel.for :as parallel-for]
            [tech.resource :as resource])
  (:import [tech.v3.datatype BooleanList LongList DoubleList ObjectList
            PrimitiveWriter PrimitiveReader BooleanReader BooleanWriter
            IntReader IntWriter LongReader LongWriter ObjectReader ObjectWriter
            DoubleReader DoubleWriter]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [java.util ArrayList List]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

;;Some overlap here between ArrayList and ObjectArrayList with the only meaningful difference
;;being that ObjectArrayList gives us access to the underlying object array.


(defn ensure-capacity
  [buffer-data ^long desired-size]
  (let [capacity (dtype-base/ecount buffer-data)
        buf-dtype (casting/host-flatten (dtype-base/elemwise-datatype buffer-data))
        buf-dtype-width (if (casting/numeric-type? buf-dtype)
                          (casting/numeric-byte-width buf-dtype)
                          1)]
    (if (> capacity desired-size)
      buffer-data
      ;;TODO - research ideal buffer growth algorithms
      ;;Once things get huge you have to be careful.
      (let [new-capacity (long (if (< (* desired-size buf-dtype-width) (* 1024 1024))
                                 (* 2 desired-size)
                                 (long (* 1.25 desired-size))))]
        (if-let [ary-buf (dtype-base/->array-buffer buffer-data)]
          (let [new-buffer (dtype-cmc/make-container :jvm-heap (.datatype ary-buf) new-capacity)]
            (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
            new-buffer)
          (let [native-buf (dtype-base/->native-buffer buffer-data)
                new-buffer (dtype-cmc/make-container :native-heap (.datatype native-buf)
                                                     new-capacity
                                                     {:endianness (.endianness native-buf)})]
            (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
            new-buffer))))))



(defn set-capacity
  [buffer-data ^long new-capacity]
  (let [capacity (dtype-base/ecount buffer-data)]
    (if-not (== capacity new-capacity)
      buffer-data
      ;;TODO - research ideal buffer growth algorithms
      ;;Once things get huge you have to be careful.
      (if-let [ary-buf (dtype-base/->array-buffer buffer-data)]
        (let [new-buffer (dtype-cmc/make-container :jvm-heap (.datatype ary-buf) new-capacity)]
          (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
          new-buffer)
        (let [native-buf (dtype-base/->native-buffer buffer-data)
              new-buffer (dtype-cmc/make-container :native-heap (.datatype native-buf)
                                                   new-capacity
                                                   {:endianness (.endianness native-buf)})]
          (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
          new-buffer)))))



(deftype BooleanListImpl [^:unsynchronized-mutable buffer
                          ^:unsynchronized-mutable ^long ptr
                          ^:unsynchronized-mutable ^PrimitiveWriter writer
                          ^:unsynchronized-mutable ^PrimitiveReader reader]
  BooleanReader
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readBoolean reader idx))
  BooleanWriter
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeBoolean writer idx value))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [this]
    (dtype-proto/->array-buffer (dtype-base/sub-buffer buffer 0 ptr)))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [this]
    (dtype-proto/->native-buffer (dtype-base/sub-buffer buffer 0 ptr)))
  BooleanList
  (ensureCapacity [item new-size]
    (let [new-buf (ensure-capacity buffer new-size)]
      (when-not (identical? new-buf buffer)
        (do
          (set! buffer new-buf)
          (let [reader-writer (dtype-base/->writer new-buf)]
            (set! writer reader-writer)
            (set! reader reader-writer))))))
  (addBoolean [item value]
    (.ensureCapacity item ptr)
    (.writeBoolean writer ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addAll [item coll]
    (let [item-ecount (dtype-base/ecount coll)]
      (.ensureCapacity item (+ ptr item-ecount))
      (if-let [data-buf (or (dtype-base/->array-buffer coll)
                            (dtype-base/->native-buffer coll))]
        (do
          (dtype-cmc/copy! data-buf (dtype-base/sub-buffer buffer ptr item-ecount))
          (set! ptr (+ ptr item-ecount)))
        (parallel-for/doiter
         data coll
         (.add item data))))))


(defn boolean-list
  (^BooleanList [initial-container]
   (let [rw (dtype-base/->reader initial-container)]
     (BooleanListImpl. initial-container 0 rw rw)))
  (^BooleanList []
   (boolean-list (dtype-cmc/make-container :boolean 10))))



(deftype LongListImpl [^:unsynchronized-mutable buffer
                      ^:unsynchronized-mutable ^long ptr
                      ^:unsynchronized-mutable ^PrimitiveWriter writer
                      ^:unsynchronized-mutable ^PrimitiveReader reader]
  LongReader
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readLong reader idx))
  LongWriter
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeLong writer idx value))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [this]
    (dtype-proto/->array-buffer (dtype-base/sub-buffer buffer 0 ptr)))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [this]
    (dtype-proto/->native-buffer (dtype-base/sub-buffer buffer 0 ptr)))

  LongList
  (ensureCapacity [item new-size]
    (let [new-buf (ensure-capacity buffer new-size)]
      (when-not (identical? new-buf buffer)
        (do
          (set! buffer new-buf)
          (let [reader-writer (dtype-base/->writer new-buf)]
            (set! writer reader-writer)
            (set! reader reader-writer))))))
  (addLong [item value]
    (.ensureCapacity item ptr)
    (.writeLong writer ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addAll [item coll]
    (let [item-ecount (dtype-base/ecount coll)]
      (.ensureCapacity item (+ ptr item-ecount))
      (if-let [data-buf (or (dtype-base/->array-buffer coll)
                            (dtype-base/->native-buffer coll))]
        (do
          (dtype-cmc/copy! data-buf (dtype-base/sub-buffer buffer ptr item-ecount))
          (set! ptr (+ ptr item-ecount)))
        (parallel-for/doiter
         data coll
         (.add item data))))))


(defn long-list
  (^LongList [initial-container]
   (let [rw (dtype-base/->reader initial-container)]
     (LongListImpl. initial-container 0 rw rw)))
  (^LongList []
   (long-list (dtype-cmc/make-container :int64 10))))



(deftype DoubleListImpl [^:unsynchronized-mutable buffer
                         ^:unsynchronized-mutable ^long ptr
                         ^:unsynchronized-mutable ^PrimitiveWriter writer
                         ^:unsynchronized-mutable ^PrimitiveReader reader]
  DoubleReader
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readDouble reader idx))
  DoubleWriter
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeDouble writer idx value))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [this]
    (dtype-proto/->array-buffer (dtype-base/sub-buffer buffer 0 ptr)))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [this]
    (dtype-proto/->native-buffer (dtype-base/sub-buffer buffer 0 ptr)))

  DoubleList
  (ensureCapacity [item new-size]
    (let [new-buf (ensure-capacity buffer new-size)]
      (when-not (identical? new-buf buffer)
        (do
          (set! buffer new-buf)
          (let [reader-writer (dtype-base/->writer new-buf)]
            (set! writer reader-writer)
            (set! reader reader-writer))))))
  (addDouble [item value]
    (.ensureCapacity item ptr)
    (.writeDouble writer ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addAll [item coll]
    (let [item-ecount (dtype-base/ecount coll)]
      (.ensureCapacity item (+ ptr item-ecount))
      (if-let [data-buf (or (dtype-base/->array-buffer coll)
                            (dtype-base/->native-buffer coll))]
        (do
          (dtype-cmc/copy! data-buf (dtype-base/sub-buffer buffer ptr item-ecount))
          (set! ptr (+ ptr item-ecount)))
        (parallel-for/doiter
         data coll
         (.add item data))))))


(defn double-list
  (^DoubleList [initial-container]
   (let [rw (dtype-base/->reader initial-container)]
     (DoubleListImpl. initial-container 0 rw rw)))
  (^DoubleList []
   (double-list (dtype-cmc/make-container :float64 10))))


(deftype ObjectListImpl [^:unsynchronized-mutable buffer
                         ^:unsynchronized-mutable ^long ptr
                         ^:unsynchronized-mutable ^PrimitiveWriter writer
                         ^:unsynchronized-mutable ^PrimitiveReader reader]
  ObjectReader
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readObject reader idx))
  ObjectWriter
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeObject writer idx value))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this]
    (dtype-proto/convertible-to-array-buffer? buffer))
  (->array-buffer [this]
    (dtype-proto/->array-buffer (dtype-base/sub-buffer buffer 0 ptr)))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [this]
    (dtype-proto/->native-buffer (dtype-base/sub-buffer buffer 0 ptr)))

  ObjectList
  (ensureCapacity [item new-size]
    (let [new-buf (ensure-capacity buffer new-size)]
      (when-not (identical? new-buf buffer)
        (do
          (set! buffer new-buf)
          (let [reader-writer (dtype-base/->writer new-buf)]
            (set! writer reader-writer)
            (set! reader reader-writer))))))
  (addObject [item value]
    (.ensureCapacity item ptr)
    (.writeObject writer ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addAll [item coll]
    (let [item-ecount (dtype-base/ecount coll)]
      (.ensureCapacity item (+ ptr item-ecount))
      (if-let [data-buf (or (dtype-base/->array-buffer coll)
                            (dtype-base/->native-buffer coll))]
        (do
          (dtype-cmc/copy! data-buf (dtype-base/sub-buffer buffer ptr item-ecount))
          (set! ptr (+ ptr item-ecount)))
        (parallel-for/doiter
         data coll
         (.add item data))))))


(defn object-list
  (^ObjectList [initial-container]
   (let [rw (dtype-base/->reader initial-container)]
     (ObjectListImpl. initial-container 0 rw rw)))
  (^ObjectList []
   (object-list (dtype-cmc/make-container :int64 10))))
