(ns tech.libs.neanderthal
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.tensor :as dtt]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.resource :as resource])
  (:import [uncomplicate.neanderthal.internal.api Block]
           [uncomplicate.commons.core Info]))


(extend-type Info
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [this]
    (let [{:keys [entry-type]} (.info this)]
      (if-let [retval (get {Double/TYPE :float64
                            Float/TYPE :float32
                            Long/TYPE :int64
                            Integer/TYPE :int32}
                           entry-type)]
        retval
        (throw (Exception. "Unrecognized type.")))))
  dtype-proto/PCountable
  (ecount [this] (:dim (.info this)))
  dtype-proto/PShape
  (shape [this] (let [info (.info this)]
                  (if (contains? info :matrix-type)
                    [(:m info) (:n info)]
                    [(:dim info)])))

  dtype-proto/PToBufferDesc
  (convertible-to-buffer-desc? [item] true)
  (->buffer-descriptor [item]
    (let [item-info (.info item)
          item-dtype (dtype-proto/elemwise-datatype item)
          item-shape (dtype-proto/shape item)]
      (when-not (or (== 1 (count item-shape)) (get-in item-info [:storage :gapless]))
        (throw (Exception. "Only dense neanderthal matrixes supported")))
      (let [ninfo (dtype-base/->native-buffer item)]
        (-> {:ptr (.address ninfo)
             :datatype item-dtype
             :endianness (.endianness ninfo)
             :shape item-shape
             ;;TVM needs the device type
             :device-type (:device item-info)
             :strides (mapv #(* (casting/numeric-byte-width item-dtype) %)
                            (if (= 2 (count item-shape))
                              (let [item-strides
                                    [(if (= :row (:layout item-info))
                                       (second item-shape)
                                       (first item-shape))
                                     1]]
                                (if (= :column (:layout item-info))
                                  (reverse item-strides)
                                  item-strides))
                              [(:stride item-info)]))}
            (resource/track (constantly item))))))

  dtype-proto/PToPrimitiveIO
  (convertible-to-primitive-io? [item] (= :cpu (:device (.info item))))
  (->primitive-io [item options]
    (dtype-proto/->primitive-io (dtype-proto/as-tensor item)))
  dtype-proto/PToTensor
  (as-tensor [item]
    (dtt/buffer-descriptor->tensor (dtype-proto/->buffer-descriptor item))))


(extend-type Block
  dtype-proto/PToNativeBuffer
  (is-convertible-native-buffer? [item] true)
  (->native-buffer [item]
    (let [item-offset (.offset item)
          ptr-val (dtype-base/->native-buffer (.buffer item))]
      (-> (dtype-base/sub-buffer ptr-val item-offset)
          (resource/track (constantly item) :gc)))))
