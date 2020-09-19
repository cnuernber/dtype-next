(ns tech.v3.tensor
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.io-indexed-buffer :as indexed-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.tensor.pprint :as tens-pp]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.tensor.dimensions.shape :as dims-shape]
            [tech.v3.tensor.tensor-copy :as tens-cpy]
            [tech.resource :as resource])
  (:import [tech.v3.datatype LongNDReader PrimitiveIO PrimitiveNDIO
            ObjectReader]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [java.util List]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare construct-tensor tensor-copy!)


(deftype Tensor [buffer dimensions
                 ^long rank
                 ^LongNDReader index-system
                 ^PrimitiveIO cached-io]
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [t] (dtype-proto/elemwise-datatype buffer))
  dtype-proto/PElemwiseCast
  (elemwise-cast [t new-dtype]
    (construct-tensor (dtype-proto/elemwise-cast buffer new-dtype)
                      dimensions))
  dtype-proto/PCountable
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

  dtype-proto/PToBufferDesc
  (convertible-to-buffer-desc? [item]
    (and (dtype-proto/convertible-to-native-buffer? buffer)
         (dims/direct? dimensions)))
  (->buffer-descriptor [item]
    (let [nbuf (dtype-base/->native-buffer buffer)]
      (->
       {:ptr (.address nbuf)
        :datatype (dtype-base/elemwise-datatype buffer)
        :endianness (dtype-proto/endianness nbuf)
        :shape (dtype-base/shape item)
        :strides (mapv (partial * (casting/numeric-byte-width
                                   (dtype-base/elemwise-datatype buffer)))
                       (:strides dimensions))}
       ;;Link the descriptor itself to the native buffer in the gc
       (resource/track (constantly nbuf) :gc))))
  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [t] true)
  (->primitive-io [t]
    (if (dims/native? dimensions)
      (dtype-proto/->primitive-io buffer)
      (indexed-buffer/indexed-buffer (.indexSystem t) buffer)))
  dtype-proto/PToReader
  (convertible-to-reader? [t] (.allowsRead t))
  (->reader [t] (dtype-proto/->primitive-io t))
  dtype-proto/PToWriter
  (convertible-to-writer? [t] (.allowsWrite t))
  (->writer [t] (dtype-proto/->primitive-io t))
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
     (dtype-proto/->primitive-io t)
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
              ^PrimitiveIO offsets (dtype-base/->reader offsets)
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
  PrimitiveNDIO
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


(defn construct-tensor
  ^Tensor [buffer dimensions]
  (let [nd-desc (dims/->global->local dimensions)]
    (Tensor. buffer dimensions
             (.rank nd-desc)
             nd-desc
             (if (dtype-proto/convertible-to-primitive-io? buffer)
               (dtype-proto/->primitive-io buffer)
               (dtype-base/->reader buffer)))))


(defn tensor? [item] (instance? PrimitiveNDIO item))


(defn- default-datatype
  [datatype]
  (if (and datatype (not= :object datatype))
    datatype
    :float64))


(defn ->tensor
  ^Tensor [data & {:keys [datatype container-type]
                   :as options}]
  (let [data-shape (dtype-base/shape data)
        datatype (default-datatype
                  (or datatype (dtype-base/elemwise-datatype data)))
        container-type (or container-type :jvm-heap)
        n-elems (apply * 1 data-shape)]
    (construct-tensor
     (first
      (dtype-proto/copy-raw->item!
       data
       (dtype-cmc/make-container container-type datatype options n-elems)
       0 options))
     (dims/dimensions data-shape))))


(defn new-tensor
  [shape & {:keys [datatype container-type]
            :as options}]
  (let [datatype (default-datatype datatype)
        container-type (or container-type :jvm-heap)
        n-elems (apply * 1 shape)]
    (construct-tensor
     (dtype-cmc/make-container container-type datatype options n-elems)
     (dims/dimensions shape))))


(defn ensure-tensor
  ^PrimitiveNDIO [item]
  (if (instance? PrimitiveNDIO item)
    item
    (if-let [item (dtype-proto/as-tensor item)]
      item
      (cond
        (dtype-base/as-buffer item)
        (construct-tensor (dtype-base/as-buffer item)
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
  "Specialized copy with optimized pathways for when tensors have contiguous
  data."
  ([src dst options]
   (tens-cpy/tensor-copy! src dst options))
  ([src dst]
   (tensor-copy! src dst nil)))


(defn dimensions-dense?
  [^PrimitiveNDIO tens]
  (dims/dense? (.dimensions tens)))


(defn rows
  [^PrimitiveNDIO src]
  (errors/when-not-error (>= (.rank src) 2)
    "Tensor has too few dimensions")
  (dtype-base/slice src 1))


(defn columns
  [^PrimitiveNDIO src]
  (errors/when-not-error (>= (.rank src) 2)
    "Tensor has too few dimensions")
  (dtype-base/slice-right src (dec (.rank src))))


(defn clone
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


(defn ensure-buffer-descriptor
  "Get a buffer descriptor from the tensor.  This may copy the data.  If you want to
  ensure sharing, use the protocol ->buffer-descriptor function."
  [tens]
  (let [tens (ensure-tensor tens)]
    (if (dtype-proto/convertible-to-buffer-desc? tens)
      (dtype-proto/->buffer-descriptor tens)
      (-> (clone tens :container-type :native-heap)
          dtype-proto/->buffer-descriptor))))


(defn buffer-descriptor->tensor
  "Given a buffer descriptor, produce a tensor"
  [{:keys [ptr datatype shape strides] :as desc}]
  (when (or (not ptr)
            (= 0 (long ptr)))
    (throw (ex-info "Cannot create tensor from nil pointer."
                    {:ptr ptr})))
  (let [dtype-size (casting/numeric-byte-width datatype)]
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
      (-> (native-buffer/wrap-address ptr buffer-len datatype
                                      (dtype-proto/platform-endianness)
                                      desc)
          (construct-tensor (dims/dimensions shape strides))))))
