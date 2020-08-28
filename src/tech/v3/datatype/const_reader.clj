(ns tech.v3.datatype.const-reader
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting])
  (:import [tech.v3.datatype ObjectReader LongReader DoubleReader BooleanReader
            PrimitiveReader]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn const-reader
  (^PrimitiveReader [item n-elems]
   (let [item-dtype (dtype-base/elemwise-datatype item)
         n-elems (long n-elems)]
     (cond
       (= :boolean item-dtype)
       (let [item (boolean item)]
         (reify BooleanReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (read [rdr idx] item)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)))
       (casting/integer-type? item-dtype)
       (let [item (long item)]
         (reify LongReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (read [rdr idx] item)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)))
       (casting/float-type? item-dtype)
       (let [item (double item)]
         (reify DoubleReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (read [rdr idx] item)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)))
       :else
       (reify ObjectReader
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
         )))))
