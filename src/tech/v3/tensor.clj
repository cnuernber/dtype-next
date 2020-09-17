(ns tech.v3.tensor
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.io-indexed-buffer :as indexed-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.tensor.pprint :as tens-pp]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.tensor.dimensions.shape :as dims-shape])
  (:import [tech.v3.datatype LongNDReader PrimitiveIO PrimitiveNDIO
            ObjectReader]))


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
  (shape [t] (.shape t))
  dtype-proto/PClone
  (clone [t]
    (tensor-copy! t
                  (construct-tensor
                   (dtype-cmc/make-container (dtype-proto/elemwise-datatype t)
                                             (dtype-base/ecount t))
                   (dims/dimensions (dtype-proto/shape t)))))
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
      (select t idx)))
  (ndReadObject [t row col]
    (if (== 2 rank)
      (.readObject cached-io (.ndReadLong index-system row col))
      (select t row col)))
  (ndReadObject [t height width chan]
    (if (== 3 rank)
      (.readObject cached-io (.ndReadLong index-system height width chan))
      (select t height width chan)))
  (ndReadObjectIter [t indexes]
    (if (== (count indexes) rank)
      (.readObject cached-io (.ndReadLongIter index-system indexes))
      (apply select t indexes)))
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
      (tensor-copy! value (apply select t indexes))))

  (allowsRead [t] (.allowsRead cached-io))
  (allowsWrite [t] (.allowsWrite cached-io))
  Object
  (toString [t] (tens-pp/tensor->string t)))


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
  [item]
  (cond
    (instance? PrimitiveNDIO item)
    item
    (dtype-proto/convertible-to-reader? item)
    (->tensor item)
    :else
    (errors/throwf "Item %s is not convertible to tensor" (type item))))
