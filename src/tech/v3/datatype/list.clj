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
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.resource :as resource])
  (:import [tech.v3.datatype BooleanList LongList DoubleList ObjectList
            PrimitiveIO BooleanIO LongIO DoubleIO ObjectIO]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [clojure.lang IObj Counted Indexed IFn]
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
                                 (max (* 2 desired-size) 10)
                                 (long (* 1.25 desired-size))))]
        (if-let [ary-buf (dtype-base/->array-buffer buffer-data)]
          (let [new-buffer (dtype-cmc/make-container :jvm-heap (.datatype ary-buf)
                                                     new-capacity)]
            (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
            new-buffer)
          (let [native-buf (dtype-base/->native-buffer buffer-data)
                new-buffer (dtype-cmc/make-container
                            :native-heap (.datatype native-buf)
                            new-capacity
                            {:endianness (.endianness native-buf)
                             :resource-type (.resource-type native-buf)})]
            (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
            new-buffer))))))



(defn set-capacity
  [buffer-data ^long new-capacity]
  (let [capacity (dtype-base/ecount buffer-data)]
    (if-not (== capacity new-capacity)
      buffer-data
      (if-let [ary-buf (dtype-base/->array-buffer buffer-data)]
        (let [new-buffer (dtype-cmc/make-container :jvm-heap (.datatype ary-buf)
                                                   new-capacity)]
          (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
          new-buffer)
        (let [native-buf (dtype-base/->native-buffer buffer-data)
              new-buffer (dtype-cmc/make-container
                          :native-heap (.datatype native-buf)
                          new-capacity
                          {:endianness (.endianness native-buf)
                           :resource-type (.resource-type native-buf)})]
          (dtype-cmc/copy! buffer-data (dtype-base/sub-buffer new-buffer 0 capacity))
          new-buffer)))))


(defn- list->string
  ^String [list-item]
  (dtype-pp/buffer->string list-item "list"))



(deftype BooleanListImpl [^:unsynchronized-mutable buffer
                          ^:unsynchronized-mutable ^long ptr
                          ^:unsynchronized-mutable ^PrimitiveIO cached-io
                          metadata]
  BooleanIO
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readBoolean cached-io idx))
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeBoolean cached-io idx value))
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
          (set! cached-io (dtype-base/->io new-buf))))))
  (addBoolean [item value]
    (.ensureCapacity item ptr)
    (.writeBoolean cached-io ptr value)
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
         (.add item data)))))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (BooleanListImpl. buffer ptr cached-io metadata))
  Counted
  (count [item] (int ptr))
  Indexed
  (nth [item idx]
    (when-not (< idx ptr)
      (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                 idx ptr))))
    (.readObject item idx))
  (nth [item idx def-val]
    (if (and (>= idx 0) (< idx (.count item)))
      (.readObject item idx)
      def-val))
  IFn
  (invoke [item idx]
    (.nth item (int idx)))
  (invoke [item idx value]
    (let [idx (long idx)]
      (when-not (< idx ptr)
        (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                   idx ptr))))
      (.writeObject item idx value)))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  Object
  (toString [buffer]
    (list->string buffer)))


(defn boolean-list
  (^BooleanList [initial-container ^long ptr]
   (let [rw (dtype-base/->reader initial-container)]
     (BooleanListImpl. initial-container ptr rw {})))
  (^BooleanList []
   (boolean-list (dtype-cmc/make-container :boolean 10) 0)))

(dtype-pp/implement-tostring-print BooleanListImpl)

(deftype LongListImpl [^:unsynchronized-mutable buffer
                       ^:unsynchronized-mutable ^long ptr
                       ^:unsynchronized-mutable ^PrimitiveIO cached-io
                       metadata]
  LongIO
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readLong cached-io idx))
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeLong cached-io idx value))
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
          (set! cached-io (dtype-base/->io new-buf))))))
  (addLong [item value]
    (.ensureCapacity item ptr)
    (.writeLong cached-io ptr value)
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
         (.add item data)))))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (LongListImpl. buffer ptr cached-io metadata))
  Counted
  (count [item] (int ptr))
  Indexed
  (nth [item idx]
    (when-not (< idx ptr)
      (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                 idx ptr))))
    (.readObject item idx))
  (nth [item idx def-val]
    (if (and (>= idx 0) (< idx (.count item)))
      (.readObject item idx)
      def-val))
  IFn
  (invoke [item idx]
    (.nth item (int idx)))
  (invoke [item idx value]
    (let [idx (long idx)]
      (when-not (< idx ptr)
        (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                   idx ptr))))
      (.writeObject item idx value)))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  Object
  (toString [buffer]
    (list->string buffer)))


(defn long-list
  (^LongList [initial-container ^long ptr]
   (let [rw (dtype-base/->io initial-container)]
     (LongListImpl. initial-container ptr rw {})))
  (^LongList []
   (long-list (dtype-cmc/make-container :int64 10) 0)))


(dtype-pp/implement-tostring-print LongListImpl)


(deftype DoubleListImpl [^:unsynchronized-mutable buffer
                         ^:unsynchronized-mutable ^long ptr
                         ^:unsynchronized-mutable ^PrimitiveIO cached-io
                         metadata]
  DoubleIO
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readDouble cached-io idx))
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeDouble cached-io idx value))
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
          (set! cached-io (dtype-base/->io new-buf))))))
  (addDouble [item value]
    (.ensureCapacity item ptr)
    (.writeDouble cached-io ptr value)
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
         (.add item data)))))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (DoubleListImpl. buffer ptr cached-io metadata))
  Counted
  (count [item] (int ptr))
  Indexed
  (nth [item idx]
    (when-not (< idx ptr)
      (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                 idx ptr))))
    (.readObject item idx))
  (nth [item idx def-val]
    (if (and (>= idx 0) (< idx (.count item)))
      (.readObject item idx)
      def-val))
  IFn
  (invoke [item idx]
    (.nth item (int idx)))
  (invoke [item idx value]
    (let [idx (long idx)]
      (when-not (< idx ptr)
        (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                   idx ptr))))
      (.writeObject item idx value)))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  Object
  (toString [buffer]
    (list->string buffer)))


(defn double-list
  (^DoubleList [initial-container ^long ptr]
   (let [rw (dtype-base/->io initial-container)]
     (DoubleListImpl. initial-container ptr rw {})))
  (^DoubleList []
   (double-list (dtype-cmc/make-container :float64 10) 0)))


(dtype-pp/implement-tostring-print DoubleListImpl)


(deftype ObjectListImpl [^:unsynchronized-mutable buffer
                         ^:unsynchronized-mutable ^long ptr
                         ^:unsynchronized-mutable ^PrimitiveIO cached-io
                         metadata]
  ObjectIO
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (read [this idx]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.readObject cached-io idx))
  (write [this idx value]
    (when-not (< idx ptr)
      (throw (Exception. "idx out of range")))
    (.writeObject cached-io idx value))
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
          (set! cached-io (dtype-base/->io new-buf))))))
  (addObject [item value]
    (.ensureCapacity item ptr)
    (.writeObject cached-io ptr value)
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
         (.add item data)))))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (ObjectListImpl. buffer ptr cached-io metadata))
  Counted
  (count [item] (int ptr))
  Indexed
  (nth [item idx]
    (when-not (< idx ptr)
      (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                 idx ptr))))
    (.readObject item idx))
  (nth [item idx def-val]
    (if (and (>= idx 0) (< idx (.count item)))
      (.readObject item idx)
      def-val))
  IFn
  (invoke [item idx]
    (.nth item (int idx)))
  (invoke [item idx value]
    (let [idx (long idx)]
      (when-not (< idx ptr)
        (throw (IndexOutOfBoundsException. (format "idx %s, n-elems %s"
                                                   idx ptr))))
      (.writeObject item idx value)))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  Object
  (toString [buffer]
    (list->string buffer)))


(defn object-list
  (^ObjectList [initial-container ^long ptr]
   (let [rw (dtype-base/->io initial-container)]
     (ObjectListImpl. initial-container ptr rw {})))
  (^ObjectList []
   (object-list (dtype-cmc/make-container :int64 16) 0)))


(dtype-pp/implement-tostring-print ObjectListImpl)


(defn wrap-container
  ^List [container]
  (let [dtype (dtype-base/elemwise-datatype container)]
    (cond
      (casting/integer-type? dtype)
      (long-list container (dtype-base/ecount container))
      (casting/float-type? dtype)
      (double-list container (dtype-base/ecount container))
      :else
      (object-list container (dtype-base/ecount container)))))


(defn empty-list
  ^List [datatype]
  (let [container (dtype-cmc/make-container :jvm-heap datatype 16)]
    (cond
      (casting/integer-type? datatype)
      (long-list container 0)
      (casting/float-type? datatype)
      (double-list container 0)
      :else
      (object-list container 0))))


(defmethod dtype-proto/make-container :list
  [container-type datatype elem-seq-or-count options]
  (if (or (number? elem-seq-or-count)
          (dtype-proto/convertible-to-reader? elem-seq-or-count))
    (-> (dtype-cmc/make-container :jvm-heap datatype elem-seq-or-count options)
        (wrap-container))
    (let [retval (empty-list datatype)]
      (parallel-for/doiter
       item elem-seq-or-count
       (.add retval item))
      retval)))
