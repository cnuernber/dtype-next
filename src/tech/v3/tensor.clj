(ns tech.v3.tensor
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.io-indexed-buffer :as indexed-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.tensor.dimensions :as dims])
  (:import [tech.v3.datatype LongNDReader PrimitiveIO PrimitiveNDIO]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare tensor->string
         construct-tensor
         select
         tensor-copy!)


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
  (toString [t] (tensor->string t)))


(defn construct-tensor
  ^Tensor [buffer dimensions]
  (let [nd-desc (dims/->global->local dimensions)]
    (Tensor. buffer dimensions
             (.rank nd-desc)
             nd-desc
             (if (dtype-proto/convertible-to-primitive-io? buffer)
               (dtype-proto/->primitive-io buffer)
               (dtype-base/->reader buffer)))))
