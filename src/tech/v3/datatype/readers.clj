(ns tech.v3.datatype.readers
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base])
  (:import [tech.v3.datatype ObjectReader PrimitiveReader]))


(defn const-reader
  (^PrimitiveReader [item n-elems]
   (let [item-dtype (dtype-base/elemwise-datatype item)
         n-elems (long n-elems)]
     (reify
       ObjectReader
       (elemwiseDatatype [rdr] item-dtype)
       (lsize [rdr] n-elems)
       (read [rdr _idx] item)
       dtype-proto/PConstantTimeMinMax
       (has-constant-time-min-max? [this] true)
       (constant-time-min [this] item)
       (constant-time-max [this] item)
       ;; dtype-proto/PRangeConvertible
       ;; (convertible-to-range? [this]
       ;;   (and (== 1 n-elems)
       ;;        (instance? Number item)))
       ;; (->range [this# options#]
       ;;   (make-single-elem-range ~datatype item#))
       ))))
