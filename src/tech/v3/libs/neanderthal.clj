(ns tech.v3.libs.neanderthal
  "Implementation of the various datatype protocols for neanderthal datatypes.  Users
  must require this to enable ->reader and as-tensor functionality for neanderthal
  datatypes."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.tensor :as dtt]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.resource :as resource]
            ;;binds nio buffers to datatype system
            [tech.v3.datatype.nio-buffer]
            ;;required so the import statements below work.
            [uncomplicate.commons.core]
            [uncomplicate.neanderthal.native :as n-native])
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
  dtype-proto/PDatatype
  (datatype [this]
    (dtype-proto/datatype (dtype-proto/as-tensor this)))
  dtype-proto/PECount
  (ecount [this] (:dim (.info this)))
  dtype-proto/PShape
  (shape [this] (let [info (.info this)]
                  (if (contains? info :matrix-type)
                    [(:m info) (:n info)]
                    [(:dim info)])))

  dtype-proto/PToNDBufferDesc
  (convertible-to-nd-buffer-desc? [item] true)
  (->nd-buffer-descriptor [item]
    (let [item-info (.info item)
          item-dtype (dtype-proto/elemwise-datatype item)
          item-shape (dtype-proto/shape item)]
      (when-not (or (== 1 (count item-shape)) (get-in item-info [:storage :gapless]))
        (throw (Exception. "Only dense neanderthal matrixes supported")))
      (let [ninfo (dtype-base/->native-buffer item)]
        {:ptr (.address ninfo)
         :elemwise-datatype item-dtype
         :datatype {:container-type :tensor
                    :elemwise-datatype item-dtype}
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
                          [(:stride item-info)]))
         ;;This allows gc to see that the neanderthal source data is still referenced
         :native-buffer item})))

  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] (= :cpu (:device (.info item))))
  (->buffer [item]
    (dtype-proto/->buffer (dtype-proto/as-tensor item)))
  dtype-proto/PToTensor
  (as-tensor [item]
    (dtt/nd-buffer-descriptor->tensor (dtype-proto/->nd-buffer-descriptor item))))


(extend-type Block
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [item] true)
  (->native-buffer [item]
    (let [item-offset (.offset item)
          ptr-val (-> (dtype-base/->native-buffer (.buffer item))
                      (native-buffer/set-native-datatype
                       (dtype-base/elemwise-datatype
                        item)))]
      (-> (dtype-base/sub-buffer ptr-val item-offset)
          (resource/chain-resources item)))))


(defn tensor->matrix
  ([tens layout datatype]
   (let [tshape (dtype-base/shape tens)
         _ (errors/when-not-errorf (== 2 (count tshape))
             "Only 2D tensors can transform to neanderthal matrix")
         [n-rows n-cols] tshape
         layout (or layout :column)
         nmat (case (or datatype (dtype-base/elemwise-datatype tens))
                :float64
                (n-native/dge n-rows n-cols {:layout layout})
                :float32
                (n-native/fge n-rows n-cols {:layout layout}))
         ntens (dtt/as-tensor nmat)]
     (dt-cmc/copy! tens ntens)
     nmat))
  ([tens]
   (tensor->matrix tens nil nil)))


(defn datatype->native-factory
  [dtype]
  (case dtype
    :float64
    (n-native/factory-by-type Double/TYPE)
    :float32
    (n-native/factory-by-type Float/TYPE)))
