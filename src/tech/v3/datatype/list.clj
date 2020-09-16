(ns tech.v3.datatype.list
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :refer [check-idx] :as errors]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.pprint :as dtype-pp])
  (:import [tech.v3.datatype PrimitiveList PrimitiveIO]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [clojure.lang IObj Counted Indexed IFn]
           [java.util ArrayList List]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

;;Some overlap here between ArrayList and ObjectArrayList with the only meaningful difference
;;being that ObjectArrayList gives us access to the underlying object array.


(defn ensure-capacity
  ([buffer-data ^long desired-size ^long capacity]
   (if (> capacity desired-size)
     buffer-data
     ;;TODO - research ideal buffer growth algorithms
     ;;Once things get huge you have to be careful.
     (let [new-capacity (long (if (< desired-size (* 1024 1024))
                                (max (* 2 desired-size) 10)
                                (long (* 1.25 desired-size))))]
       (if-let [ary-buf (dtype-base/as-array-buffer buffer-data)]
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


(defn- list->string
  ^String [list-item]
  (dtype-pp/buffer->string list-item "list"))


(deftype ListImpl [^:unsynchronized-mutable buffer
                   ^:unsynchronized-mutable ^long capacity
                   ^:unsynchronized-mutable ^long ptr
                   ^:unsynchronized-mutable ^PrimitiveIO cached-io
                   metadata]
  PrimitiveIO
  (elemwiseDatatype [this] (dtype-base/elemwise-datatype buffer))
  (lsize [this] ptr)
  (readBoolean [this idx] (check-idx idx ptr) (.readBoolean cached-io idx))
  (readByte [this idx] (check-idx idx ptr) (.readByte cached-io idx))
  (readShort [this idx] (check-idx idx ptr) (.readShort cached-io idx))
  (readChar [this idx] (check-idx idx ptr) (.readChar cached-io idx))
  (readInt [this idx] (check-idx idx ptr) (.readInt cached-io idx))
  (readLong [this idx] (check-idx idx ptr) (.readLong cached-io idx))
  (readFloat [this idx] (check-idx idx ptr) (.readFloat cached-io idx))
  (readDouble [this idx] (check-idx idx ptr) (.readDouble cached-io idx))
  (readObject [this idx] (check-idx idx ptr) (.readObject cached-io idx))
  (writeBoolean [this idx val] (check-idx idx ptr) (.writeBoolean cached-io idx val))
  (writeByte [this idx val] (check-idx idx ptr) (.writeByte cached-io idx val))
  (writeShort [this idx val] (check-idx idx ptr) (.writeShort cached-io idx val))
  (writeChar [this idx val] (check-idx idx ptr) (.writeChar cached-io idx val))
  (writeInt [this idx val] (check-idx idx ptr) (.writeInt cached-io idx val))
  (writeLong [this idx val] (check-idx idx ptr) (.writeLong cached-io idx val))
  (writeFloat [this idx val] (check-idx idx ptr) (.writeFloat cached-io idx val))
  (writeDouble [this idx val] (check-idx idx ptr) (.writeDouble cached-io idx val))
  (writeObject [this idx val] (check-idx idx ptr) (.writeObject cached-io idx val))
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
  PrimitiveList
  (ensureCapacity [item new-size]
    (let [new-buf (ensure-capacity buffer new-size capacity)]
      (when-not (identical? new-buf buffer)
        (do
          (set! buffer new-buf)
          (set! capacity (dtype-base/ecount new-buf))
          (set! cached-io (dtype-base/->io new-buf))))))
  (addBoolean [this value]
    ;;Check is done here to avoid fn call when not necessary
    (when (>= ptr capacity) (.ensureCapacity this ptr))
    (.writeBoolean cached-io ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addDouble [this value]
    (when (>= ptr capacity) (.ensureCapacity this ptr))
    (.writeDouble cached-io ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addLong [this value]
    (when (>= ptr capacity) (.ensureCapacity this ptr))
    (.writeLong cached-io ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addObject [this value]
    (when (>= ptr capacity) (.ensureCapacity this ptr))
    (.writeObject cached-io ptr value)
    (set! ptr (unchecked-inc ptr)))
  (addAll [item coll]
    (if-let [data-buf (or (dtype-base/as-buffer coll)
                          (dtype-base/->reader coll))]
      (let [item-ecount (dtype-base/ecount data-buf)]
        (.ensureCapacity item (+ ptr item-ecount))
        (dtype-cmc/copy! data-buf (dtype-base/sub-buffer buffer ptr item-ecount))
        (set! ptr (+ ptr item-ecount)))
      (parallel-for/consume! #(.add item %) coll))
    true)
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (ListImpl. buffer capacity ptr cached-io metadata))
  Counted
  (count [item] (int ptr))
  Indexed
  (nth [item idx]
    (check-idx idx ptr)
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
      (check-idx idx ptr)
      (.writeObject item idx value)))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  Object
  (toString [buffer]
    (list->string buffer)))


(dtype-pp/implement-tostring-print ListImpl)


(defn make-list
  (^PrimitiveList [initial-container ^long ptr]
   (let [rw (dtype-base/->reader initial-container)]
     (ListImpl. initial-container (dtype-base/ecount initial-container) ptr rw {})))
  (^PrimitiveList [datatype]
   (make-list (dtype-cmc/make-container datatype 16) 0)))


(defn wrap-container
  ^List [container]
  (make-list container (dtype-base/ecount container)))


(defn empty-list
  ^List [datatype]
  (make-list datatype))


(defmethod dtype-proto/make-container :list
  [container-type datatype options elem-seq-or-count]
  (if (or (number? elem-seq-or-count)
          (dtype-proto/convertible-to-reader? elem-seq-or-count))
    (-> (dtype-cmc/make-container :jvm-heap datatype options elem-seq-or-count)
        (wrap-container))
    (let [retval (empty-list datatype)]
      (parallel-for/consume! #(.add retval %) elem-seq-or-count)
      retval)))
