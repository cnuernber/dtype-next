(ns tech.v3.tensor
  "ND bindings for the tech.v3.datatype system.  A Tensor is conceptually just a tuple
  of a buffer and an index operator that is capable of converting indexes in ND space
  into a single long index into the buffer.  Tensors implementent the
  tech.v3.datatype.NDBuffer interface and outside this file ND objects are expected to
  simply implement that interface.

  This system relies heavily on the tech.v3.tensor.dimensions namespace to provide the
  optimized indexing operator from ND space to buffer space and back."
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.io-indexed-buffer :as indexed-buffer]
            [tech.v3.datatype.const-reader :refer [const-reader]]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.tensor.pprint :as tens-pp]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.tensor.dimensions.shape :as dims-shape]
            [tech.v3.tensor.tensor-copy :as tens-cpy]
            [tech.v3.datatype.export-symbols :as export-symbols]
            [tech.resource :as resource])
  (:import [tech.v3.datatype LongNDReader Buffer NDBuffer
            ObjectReader]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [java.util List]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare construct-tensor tensor-copy!)


(deftype Tensor [buffer dimensions
                 ^long rank
                 ^LongNDReader index-system
                 ^Buffer cached-io]
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [t] (dtype-proto/elemwise-datatype buffer))
  dtype-proto/PElemwiseCast
  (elemwise-cast [t new-dtype]
    (construct-tensor (dtype-proto/elemwise-cast buffer new-dtype)
                      dimensions))
  dtype-proto/PDatatype
  (datatype [this] :tensor)
  dtype-proto/PECount
  (ecount [t] (dims/ecount dimensions))
  dtype-proto/PShape
  (shape [t] (.shape index-system))
  dtype-proto/PClone
  (clone [t]
    (tensor-copy! t
                  (construct-tensor
                   (dtype-cmc/make-container (dtype-proto/elemwise-datatype t)
                                             (dtype-base/ecount t))
                   (dims/dimensions (dtype-proto/shape t)))))

  dtype-proto/PToNDBufferDesc
  (convertible-to-nd-buffer-desc? [item]
    (and (dtype-proto/convertible-to-native-buffer? buffer)
         (dims/direct? dimensions)))
  (->nd-buffer-descriptor [item]
    (let [nbuf (dtype-base/->native-buffer buffer)]
      (->
       {:ptr (.address nbuf)
        :datatype :tensor
        :elemwise-datatype (dtype-base/elemwise-datatype buffer)
        :endianness (dtype-proto/endianness nbuf)
        :shape (dtype-base/shape item)
        :strides (mapv (partial * (casting/numeric-byte-width
                                   (dtype-base/elemwise-datatype buffer)))
                       (:strides dimensions))}
       ;;Link the descriptor itself to the native buffer in the gc
       (resource/track (constantly nbuf) :gc))))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [t] true)
  (->buffer [t]
    (if (dims/native? dimensions)
      (dtype-proto/->buffer buffer)
      (indexed-buffer/indexed-buffer (.indexSystem t) buffer)))
  dtype-proto/PToReader
  (convertible-to-reader? [t] (.allowsRead t))
  (->reader [t] (dtype-proto/->buffer t))
  dtype-proto/PToWriter
  (convertible-to-writer? [t] (.allowsWrite t))
  (->writer [t] (dtype-proto/->buffer t))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [t]
    (and (dtype-proto/convertible-to-array-buffer? buffer)
         (dims/native? dimensions)))
  (->array-buffer [t] (dtype-proto/->array-buffer buffer))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [t]
    (and (dtype-proto/convertible-to-native-buffer? buffer)
         (dims/native? dimensions)))
  (->native-buffer [t] (dtype-proto/->native-buffer buffer))
  dtype-proto/PTensor
  (reshape [t new-shape]
    (construct-tensor
     (dtype-proto/->buffer t)
     (dims/dimensions new-shape)))
  (select [t select-args]
    (let [{buf-offset :elem-offset
           buf-len :buffer-ecount
           :as new-dims}
          (apply dims/select dimensions select-args)
          buf-offset (long buf-offset)
          new-buffer (if-not (and (== buf-offset 0)
                                  (or (not buf-len)
                                      (== (dtype-base/ecount buffer) (long buf-len))))
                       (dtype-base/sub-buffer buffer buf-offset buf-len)
                       buffer)]
    (construct-tensor new-buffer new-dims)))
  (transpose [t transpose-vec]
    (construct-tensor buffer (dims/transpose dimensions transpose-vec)))
  (broadcast [t bcast-shape]
    (let [tens-shape (dtype-base/shape t)
          n-tens-elems (dtype-base/ecount t)
          n-bcast-elems (dims-shape/ecount bcast-shape)
          num-tens-shape (count tens-shape)]
      (when-not (every? number? bcast-shape)
        (throw (ex-info "Broadcast shapes must only be numbers" {})))
      (when-not (>= n-bcast-elems
                    n-tens-elems)
        (throw (ex-info
                (format "Improper broadcast shape (%s), smaller than tens (%s)"
                        bcast-shape tens-shape)
                {})))
      (when-not (every? (fn [[item-dim bcast-dim]]
                          (= 0 (rem (int bcast-dim)
                                    (int item-dim))))
                        (map vector tens-shape (take-last num-tens-shape bcast-shape)))
        (throw (ex-info
                (format "Broadcast shape (%s) is not commensurate with tensor shape %s"
                        bcast-shape tens-shape)
                {})))
      (construct-tensor buffer (dims/broadcast dimensions bcast-shape))))
  (rotate [t offset-vec]
    (construct-tensor buffer
                      (dims/rotate dimensions
                                   (mapv #(* -1 (long %)) offset-vec))))
  (slice [tens slice-dims right?]
    (let [t-shape (dtype-base/shape tens)
          n-shape (count t-shape)
          slice-dims (long slice-dims)]
      (when-not (<= slice-dims n-shape)
        (throw (ex-info (format "Slice operator n-dims out of range: %s:%s"
                                slice-dims t-shape)
                        {})))
      (if (== slice-dims n-shape)
        (dtype-base/->reader tens)
        (let [{:keys [dimensions offsets]}
              (if right?
                (dims/slice-right dimensions slice-dims)
                (dims/slice dimensions slice-dims))
              ^Buffer offsets (dtype-base/->reader offsets)
              n-offsets (.lsize offsets)
              tens-buf buffer
              buf-ecount (:buffer-ecount dimensions)]
          (reify ObjectReader
            (lsize [rdr] n-offsets)
            (readObject [rdr idx]
              (construct-tensor (dtype-base/sub-buffer
                                 tens-buf
                                 (.readLong offsets idx)
                                 buf-ecount)
                                dimensions)))))))
  (mget [t idx-seq]
    (.ndReadObjectIter t idx-seq))
  (mset! [t idx-seq value]
    (.ndWriteObjectIter t idx-seq value))
  NDBuffer
  (buffer [t] buffer)
  (dimensions [t] dimensions)
  (indexSystem [t] index-system)
  (bufferIO [t] cached-io)

  (ndReadBoolean [t idx]
    (.readBoolean cached-io (.ndReadLong index-system idx)))
  (ndReadBoolean [t row col]
    (.readBoolean cached-io (.ndReadLong index-system row col)))
  (ndReadBoolean [t height width chan]
    (.readBoolean cached-io (.ndReadLong index-system height width chan)))
  (ndWriteBoolean [t idx value]
    (.writeBoolean cached-io (.ndReadLong index-system idx) value))
  (ndWriteBoolean [t row col value]
    (.writeBoolean cached-io (.ndReadLong index-system row col) value))
  (ndWriteBoolean [t height width chan value]
    (.writeBoolean cached-io (.ndReadLong index-system height width chan)
                   value))

  (ndReadLong [t idx]
    (.readLong cached-io (.ndReadLong index-system idx)))
  (ndReadLong [t row col]
    (.readLong cached-io (.ndReadLong index-system row col)))
  (ndReadLong [t height width chan]
    (.readLong cached-io (.ndReadLong index-system height width chan)))
  (ndWriteLong [t idx value]
    (.writeLong cached-io (.ndReadLong index-system idx) value))
  (ndWriteLong [t row col value]
    (.writeLong cached-io (.ndReadLong index-system row col) value))
  (ndWriteLong [t height width chan value]
    (.writeLong cached-io (.ndReadLong index-system height width chan)
                value))

  (ndReadDouble [t idx]
    (.readDouble cached-io (.ndReadLong index-system idx)))
  (ndReadDouble [t row col]
    (.readDouble cached-io (.ndReadLong index-system row col)))
  (ndReadDouble [t height width chan]
    (.readDouble cached-io (.ndReadLong index-system height width chan)))
  (ndWriteDouble [t idx value]
    (.writeDouble cached-io (.ndReadLong index-system idx) value))
  (ndWriteDouble [t row col value]
    (.writeDouble cached-io (.ndReadLong index-system row col) value))
  (ndWriteDouble [t height width chan value]
    (.writeDouble cached-io (.ndReadLong index-system height width chan)
                  value))

  (ndReadObject [t idx]
    (if (== 1 rank)
      (.readObject cached-io (.ndReadLong index-system idx))
      (dtype-proto/select t [idx])))
  (ndReadObject [t row col]
    (if (== 2 rank)
      (.readObject cached-io (.ndReadLong index-system row col))
      (dtype-proto/select t [row col])))
  (ndReadObject [t height width chan]
    (if (== 3 rank)
      (.readObject cached-io (.ndReadLong index-system height width chan))
      (dtype-proto/select t [height width chan])))
  (ndReadObjectIter [t indexes]
    (if (== (count indexes) rank)
      (.readObject cached-io (.ndReadLongIter index-system indexes))
      (dtype-proto/select t indexes)))
  (ndWriteObject [t idx value]
    (if (== 1 rank)
      (.writeObject cached-io (.ndReadLong index-system idx) value)
      (tensor-copy! value (.ndReadObject t idx))))
  (ndWriteObject [t row col value]
    (if (== 2 rank)
      (.writeObject cached-io (.ndReadLong index-system row col) value)
      (tensor-copy! value (.ndReadObject t row col))))
  (ndWriteObject [t height width chan value]
    (if (== 3 rank)
      (.writeObject cached-io (.ndReadLong index-system height width chan)
                    value)
      (tensor-copy! value (.ndReadObject t height width chan))))
  (ndWriteObjectIter [t indexes value]
    (if (== (count indexes) rank)
      (.writeObject cached-io (.ndReadLongIter index-system indexes) value)
      (tensor-copy! value (dtype-proto/select t indexes))))

  (allowsRead [t] (.allowsRead cached-io))
  (allowsWrite [t] (.allowsWrite cached-io))
  (iterator [t]
    (.iterator (dtype-proto/slice t 1 false)))
  Object
  (toString [t] (tens-pp/tensor->string t)))


(dtype-pp/implement-tostring-print Tensor)


(casting/add-object-datatype! :tensor NDBuffer false)


(defn construct-tensor
  "Construct an implementation of tech.v3.datatype.NDBuffer from a buffer and
  a dimensions object.  See dimensions/dimensions."
  ^Tensor [buffer dimensions]
  (let [nd-desc (dims/->global->local dimensions)]
    (Tensor. buffer dimensions
             (.rank nd-desc)
             nd-desc
             (if (dtype-proto/convertible-to-buffer? buffer)
               (dtype-proto/->buffer buffer)
               (dtype-base/->reader buffer)))))


(defn tensor?
  "Returns true if this implements the tech.v3.datatype.NDBuffer interface."
  [item]
  (instance? NDBuffer item))


(defn tensor->buffer
  "Get the buffer from a tensor."
  [item]
  (errors/when-not-error (instance? NDBuffer item)
    "Item is not a tensor")
  (.buffer ^NDBuffer item))


(defn tensor->dimensions
  "Get the dimensions object from a tensor."
  [item]
  (errors/when-not-error (instance? NDBuffer item)
    "Item is not a tensor")
  (.dimensions ^NDBuffer item))


(defn simple-dimensions?
  "Are the dimensions of this object simple meaning read in order with no
  breaks due to striding."
  [item]
  (dims/native? (tensor->dimensions item)))


(defn dims-suitable-for-desc?
    "Are the dimensions of this object suitable for use in a buffer description?
  breaks due to striding."
  [item]
  (dims/direct? (tensor->dimensions item)))


(defn ->tensor
  "Convert some data into a tensor via copying the data.  The datatype and container
  type can be specified.  The datatype defaults to the datatype of the input data and container
  type defaults to jvm-heap."
  ^Tensor [data & {:keys [datatype container-type]
                   :as options}]
  (let [data-shape (dtype-base/shape data)
        datatype (or datatype (dtype-base/elemwise-datatype data))
        container-type (or container-type :jvm-heap)
        n-elems (apply * 1 data-shape)]
    (construct-tensor
     (first
      (dtype-proto/copy-raw->item!
       data
       (dtype-cmc/make-container container-type datatype options n-elems)
       0 options))
     (dims/dimensions data-shape))))


(defn as-tensor
  "Attempts an in-place conversion of this object to a tech.v3.datatype.NDBuffer interface.
  For a guaranteed conversion, use ensure-tensor."
  ^NDBuffer [data]
  (if (instance? NDBuffer data)
    data
    (dtype-proto/as-tensor data)))


(defn new-tensor
  "Create a new tensor with a given shape, datatype and container type.  Datatype
  defaults to :float64 if not passed in while container-type defaults to
  java-heap."
  [shape & {:keys [datatype container-type]
            :as options}]
  (let [datatype (or datatype :float64)
        container-type (or container-type :jvm-heap)
        n-elems (apply * 1 shape)]
    (construct-tensor
     (dtype-cmc/make-container container-type datatype options n-elems)
     (dims/dimensions shape))))


(defn const-tensor
  "Construct a tensor from a value and a shape.  Data is represented efficiently via a const-reader."
  [value shape]
  (let [dims (dims/dimensions shape)
        n-elems (dims/ecount dims)]
    (construct-tensor (const-reader value n-elems) dims)))


(defn ensure-tensor
  "Create an implementation of tech.v3.datatype.NDBuffer from an
  object.  If possible, represent the data in-place."
  ^NDBuffer [item]
  (if (instance? NDBuffer item)
    item
    (if-let [item (dtype-proto/as-tensor item)]
      item
      (cond
        (dtype-base/as-concrete-buffer item)
        (construct-tensor (dtype-base/as-concrete-buffer item)
                          (dims/dimensions (dtype-base/shape item)))
        (and (dtype-proto/convertible-to-reader? item)
             (= 1 (count (dtype-base/shape item))))
        (construct-tensor item (dims/dimensions (dtype-base/shape item)))
        :else
        (->tensor item)))))


;;Defaults for tensor protocols
(extend-type Object
  dtype-proto/PTensor
  (reshape [t new-shape]
    (-> (ensure-tensor t)
        (dtype-proto/reshape new-shape)))
  (select [t select-args]
    (-> (ensure-tensor t)
        (dtype-proto/select select-args)))
  (transpose [t reorder-vec]
    (-> (ensure-tensor t)
        (dtype-proto/transpose reorder-vec)))
  (broadcast [t new-shape]
    (-> (ensure-tensor t)
        (dtype-proto/broadcast new-shape)))
  (rotate [t offset-vec]
    (-> (ensure-tensor t)
        (dtype-proto/rotate offset-vec)))
  (slice [t n-dims right?]
    (-> (ensure-tensor t)
        (dtype-proto/slice n-dims right?)))
  (mget [t idx-seq]
    (errors/when-not-error
     (== 1 (count idx-seq))
     "Generic mget on reader can only have 1 dimension")
    (dtype-base/get-value t (first idx-seq)))
  (mset! [t idx-seq value]
    (errors/when-not-error
     (== 1 (count idx-seq))
     "Generic mset on reader can only have 1 dimension")
    (dtype-base/set-value! t (first idx-seq) value))
  dtype-proto/PToTensor
  (as-tensor [item] nil))


(defn tensor-copy!
  "Specialized copy with optimized pathways for when tensors have regions of contiguous
  data.  As an example consider a sub-image of a larger image.  Each row can be copied
  contiguously into a new image but there are gaps between them."
  ([src dst options]
   (let [src-argtype (arg-type src)
         src (if (= src-argtype :scalar)
               (const-tensor src (dtype-base/shape dst))
               (ensure-tensor src))]
     (tens-cpy/tensor-copy! src dst options)))
  ([src dst]
   (tensor-copy! src dst nil)))


(defn dimensions-dense?
  "Returns true of the dimensions of a tensor are dense, meaning no gaps due to
  striding."
  [^NDBuffer tens]
  (dims/dense? (.dimensions tens)))


(defn rows
  "Return the rows of the tensor in a randomly-addressable structure."
  ^List [^NDBuffer src]
  (errors/when-not-error (>= (.rank src) 2)
    "Tensor has too few dimensions")
  (dtype-base/slice src 1))


(defn columns
  "Return the columns of the tensor in a randomly-addressable structure."
  ^List [^NDBuffer src]
  (errors/when-not-error (>= (.rank src) 2)
    "Tensor has too few dimensions")
  (dtype-base/slice-right src (dec (.rank src))))


(defn clone
  "Clone a tensor via copying the tensor into a new container.  Datatype defaults
  to the datatype of the tensor and container-type defaults to :java-heap."
  [tens & {:keys [datatype
                  container-type]}]
  (let [datatype (or datatype (dtype-base/elemwise-datatype tens))
        container-type (or container-type :jvm-heap)]
    (dtype-cmc/copy! tens (new-tensor (dtype-base/shape tens)
                                      :datatype datatype
                                      :container-type container-type))))


(defn ->jvm
  "Conversion to storage that is efficient for the jvm.
  Base storage is either jvm-array or persistent-vector."
  [item & {:keys [datatype base-storage]
           :or {base-storage :persistent-vector}}]
  ;;Get the data off the device
  (let [item-shape (dtype-base/shape item)
        item-ecount (dtype-base/ecount item)
        column-len (long (last item-shape))
        n-columns (quot item-ecount column-len)
        datatype (or datatype (dtype-base/elemwise-datatype item))
        data-array (dtype-proto/->reader item)
        base-data
        (->> (range n-columns)
             (map (fn [col-idx]
                    (let [col-offset (* column-len (long col-idx))]
                      (case base-storage
                        :java-array
                        (let [retval (->
                                      (dtype-cmc/make-container datatype column-len)
                                      (dtype-cmc/->array))]
                          (dtype-cmc/copy! (dtype-base/sub-buffer
                                            data-array col-offset column-len)
                                           retval))
                        :persistent-vector
                        (->> (dtype-base/sub-buffer data-array col-offset column-len)
                             (dtype-base/->reader)
                             (vec)))))))
        partitionv (fn [& args]
                     (map vec (apply partition args)))
        partition-shape (->> (rest item-shape)
                             drop-last
                             reverse)]
    (if (> (count item-shape) 1)
      (->> partition-shape
           (reduce (fn [retval part-value]
                     (partitionv part-value retval))
                   base-data)
           vec)
      (first base-data))))


(defn ensure-nd-buffer-descriptor
  "Get a buffer descriptor from the tensor.  This may copy the data.  If you want to
  ensure sharing, use the protocol ->nd-buffer-descriptor function."
  [tens]
  (let [tens (ensure-tensor tens)]
    (if (dtype-proto/convertible-to-nd-buffer-desc? tens)
      (dtype-proto/->nd-buffer-descriptor tens)
      (-> (clone tens :container-type :native-heap)
          dtype-proto/->nd-buffer-descriptor))))


(defn nd-buffer-descriptor->tensor
  "Given a buffer descriptor, produce a tensor"
  [{:keys [ptr elemwise-datatype shape strides] :as desc}]
  (when (or (not ptr)
            (= 0 (long ptr)))
    (throw (ex-info "Cannot create tensor from nil pointer."
                    {:ptr ptr})))
  (let [dtype-size (casting/numeric-byte-width elemwise-datatype)]
    (when-not (every? #(= 0 (rem (long %)
                                 dtype-size))
                      strides)
      (throw (ex-info "Strides are not commensurate with datatype size." {})))
    (let [max-stride-idx (argops/argmax strides)
          buffer-len (* (long (dtype-base/get-value shape max-stride-idx))
                        (long (dtype-base/get-value strides max-stride-idx)))
          ;;Move strides into elem-count instead of byte-count
          strides (mapv #(quot (long %) dtype-size)
                        strides)]
      (-> (native-buffer/wrap-address ptr buffer-len elemwise-datatype
                                      (dtype-proto/platform-endianness)
                                      desc)
          (construct-tensor (dims/dimensions shape strides))))))


(export-symbols/export-symbols tech.v3.datatype.base
                               reshape
                               select
                               transpose
                               broadcast
                               rotate
                               slice
                               slice-right
                               mget
                               mset!)
