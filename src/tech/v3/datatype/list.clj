(ns tech.v3.datatype.list
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :refer [check-idx] :as errors]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.array-buffer])
  (:import [tech.v3.datatype Buffer]
           [tech.v3.datatype.array_buffer IGrowableList]
           [clojure.lang IObj Counted IFn]
           [ham_fisted ArrayLists]
           [java.util List]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- list->string
  ^String [list-item]
  (dtype-pp/buffer->string list-item "list"))


(deftype ListImpl [^:unsynchronized-mutable buffer
                   ^:unsynchronized-mutable ^long capacity
                   ^:unsynchronized-mutable ^long ptr
                   ^:unsynchronized-mutable ^Buffer cached-io
                   metadata]
  IGrowableList
  (ensureCapacity [_item new-size]
    buffer
    (if (>= capacity new-size)
      buffer
      ;;TODO - research ideal buffer growth algorithms
      ;;Once things get huge you have to be careful.
      (let [new-capacity (ArrayLists/newArrayLen new-size)
            old-buf buffer
            native-buf (dtype-base/->native-buffer old-buf)
            new-buffer (dtype-cmc/make-container
                        :native-heap (.elemwise-datatype native-buf)
                        new-capacity
                        {:endianness (.endianness native-buf)
                         :resource-type (.resource-type native-buf)})]
        (dtype-cmc/copy! old-buf (dtype-base/sub-buffer new-buffer 0 capacity))
        (set! buffer new-buffer)
        (set! capacity (long new-capacity))
        (set! cached-io (dtype-base/->buffer new-buffer))
        new-buffer)))

  Buffer
  (elemwiseDatatype [_this] (dtype-base/elemwise-datatype buffer))
  (lsize [_this] ptr)
  (allowsRead [_this] true)
  (allowsWrite [_this] true)
  (readBoolean [_this idx] (.readBoolean cached-io idx))
  (readByte [_this idx] (.readByte cached-io idx))
  (readShort [_this idx] (.readShort cached-io idx))
  (readChar [_this idx] (.readChar cached-io idx))
  (readInt [_this idx]  (.readInt cached-io idx))
  (readLong [_this idx]  (.readLong cached-io idx))
  (readFloat [_this idx]  (.readFloat cached-io idx))
  (readDouble [_this idx]  (.readDouble cached-io idx))
  (readObject [_this idx]  (.readObject cached-io idx))
  (writeBoolean [_this idx val] (.writeBoolean cached-io idx val))
  (writeByte [_this idx val] (.writeByte cached-io idx val))
  (writeShort [_this idx val] (.writeShort cached-io idx val))
  (writeChar [_this idx val] (.writeChar cached-io idx val))
  (writeInt [_this idx val] (.writeInt cached-io idx val))
  (writeLong [_this idx val] (.writeLong cached-io idx val))
  (writeFloat [_this idx val] (.writeFloat cached-io idx val))
  (writeDouble [_this idx val] (.writeDouble cached-io idx val))
  (writeObject [_this idx val] (.writeObject cached-io idx val))
  dtype-proto/PDatatype
  (datatype [_this] :list)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item _new-dtype] item)
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [_this] false)
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [_this]
    (dtype-proto/convertible-to-native-buffer? buffer))
  (->native-buffer [_this]
    (dtype-proto/->native-buffer (dtype-base/sub-buffer buffer 0 ptr)))
  dtype-proto/PClone
  (clone [_this]
    (let [new-buf (dtype-proto/clone (dtype-base/sub-buffer buffer 0 ptr))]
      (ListImpl. new-buf ptr ptr
                 (dtype-proto/->buffer new-buf) metadata)))
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
  (add [this value]
    (when (>= ptr capacity) (.ensureCapacity this ptr))
    (.writeObject cached-io ptr value)
    (set! ptr (unchecked-inc ptr))
    true)
  (addAllReducible [item coll]
    (if-let [data-buf (dtype-base/as-buffer coll)]
      (let [item-ecount (.lsize data-buf)
            new-buf (.ensureCapacity item (+ ptr item-ecount))]
        (if (> item-ecount 256)
          (do
            (dtype-cmc/copy! data-buf (dtype-base/sub-buffer ))
            (set! ptr (+ ptr item-ecount)))
          (parallel-for/doiter val coll (.add item val))))
      (parallel-for/doiter val coll (.add item val)))
    true)
  IObj
  (meta [_item] metadata)
  (withMeta [_item metadata]
    (ListImpl. buffer capacity ptr cached-io metadata))
  Counted
  (count [_item] (int ptr))
  (nth [item idx]
    (let [idx (if (< idx 0) (+ (.size item) idx) idx)]
      (.readObject item idx)))
  (nth [item idx def-val]
    (let [idx (long (if (< idx 0) (+ (.size item) idx) idx))]
      (if (and (>= idx 0) (< idx (.size item)))
        (.readObject item idx)
        def-val)))
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


(casting/add-object-datatype! :list Buffer false)


(defn make-list
  "Make a new primitive list out of a container and a ptr that indicates the
  current write position."
  (^Buffer [initial-container ^long ptr]
   (let [rw (dtype-base/->reader initial-container)]
     (ListImpl. initial-container (dtype-base/ecount initial-container) ptr rw {})))
  (^Buffer [datatype]
   (make-list (dtype-cmc/make-container datatype 16) 0)))


(defn wrap-container
  "In-place wrap an existing container.  Write ptr points to the end
  of the container so the next add* method will cause an allocation."
  ^List [container]
  (make-list container (dtype-base/ecount container)))


(defn empty-list
  "Make an empty list of a given datatype."
  ^List [datatype]
  (make-list datatype))


(defmethod dtype-proto/make-container :list
  [_container-type datatype options elem-seq-or-count]
  (if (or (number? elem-seq-or-count)
          (dtype-proto/convertible-to-reader? elem-seq-or-count))
    (-> (dtype-cmc/make-container :jvm-heap datatype options elem-seq-or-count)
        (wrap-container))
    (let [retval (empty-list datatype)]
      (parallel-for/consume! #(.add retval %) elem-seq-or-count)
      retval)))
