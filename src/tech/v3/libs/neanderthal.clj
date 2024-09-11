(ns tech.v3.neanderthal
  (:require [uncomplicate.neanderthal.native :as n-native]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.tensor :as dtt]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.base :as dtype-base]
            [clojure.tools.logging :as log]))


(try
  ;;test for uncomplicate javacpp support
  (require '[uncomplicate.clojure-cpp])
  (require '[tech.v3.libs.neanderthal-post-48])
  (catch Exception e
    (require '[tech.v3.libs.neanderthal-pre-48])))


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
