(ns tech.v3.datatype.const-reader
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.monotonic-range :as monotonic-range])
  (:import [tech.v3.datatype ObjectReader LongReader DoubleReader Buffer]
           [ham_fisted Casts]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn make-single-elem-range
  [elem]
  (let [elem (long (casting/cast elem :int64))]
    (monotonic-range/make-range
     elem
     (unchecked-inc elem))))


(defn const-reader
  "Create a new reader that only returns the item for the provided indexes."
  (^Buffer [item n-elems]
   (let [item-dtype (if item (dtype-proto/elemwise-datatype item) :object)
         n-elems (long n-elems)]
     (cond
       (identical? :boolean item-dtype)
       (let [item (Casts/booleanCast item)]
         (reify LongReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (readLong [rdr idx] (Casts/longCast item))
           (readObject [rdr idx] item)
           dtype-proto/PElemwiseReaderCast
           (elemwise-reader-cast [rdr new-dtype] rdr)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)
           dtype-proto/PRangeConvertible
           (convertible-to-range? [this]
             (and (== 1 n-elems)
                  (instance? Number item)))
           (->range [this options]
             (make-single-elem-range item))))
       (casting/integer-type? item-dtype)
       (let [item (long item)]
         (reify LongReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (readLong [rdr idx] item)
           dtype-proto/PElemwiseReaderCast
           (elemwise-reader-cast [rdr new-dtype] rdr)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)
           dtype-proto/PRangeConvertible
           (convertible-to-range? [this]
             (and (== 1 n-elems)
                  (instance? Number item)))
           (->range [this options]
             (make-single-elem-range item))))
       (casting/float-type? item-dtype)
       (let [item (double item)]
         (reify DoubleReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (readDouble [rdr idx] item)
           dtype-proto/PElemwiseReaderCast
           (elemwise-reader-cast [rdr new-dtype] rdr)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)
           dtype-proto/PRangeConvertible
           (convertible-to-range? [this]
             (and (== 1 n-elems)
                  (instance? Number item)))
           (->range [this options]
             (make-single-elem-range item))))
       :else
       (reify ObjectReader
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (readObject [rdr _idx] item)
         dtype-proto/PElemwiseReaderCast
         (elemwise-reader-cast [rdr new-dtype] rdr)
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this] true)
         (constant-time-min [this] item)
         (constant-time-max [this] item)
         dtype-proto/PRangeConvertible
         (convertible-to-range? [this]
           (and (== 1 n-elems)
                (instance? Number item)))
         (->range [this options]
           (make-single-elem-range item)))))))
