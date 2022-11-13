(ns tech.v3.datatype.const-reader
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.monotonic-range :as monotonic-range]
            [ham-fisted.api :as hamf])
  (:import [tech.v3.datatype ObjectReader LongReader DoubleReader Buffer
            BooleanReader]
           [ham_fisted Casts ChunkedList]))

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
         (reify BooleanReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (readObject [rdr idx] item)
           (subBuffer [rdr sidx eidx]
             (ChunkedList/sublistCheck sidx eidx n-elems)
             (const-reader item (- eidx sidx)))
           (reduce [this rfn init]
             (loop [init init
                    idx 0]
               (if (and (< idx n-elems) (not (reduced? init)))
                 (recur (rfn init item) (unchecked-inc idx))
                 init)))
           dtype-proto/PElemwiseReaderCast
           (elemwise-reader-cast [rdr new-dtype] rdr)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)
           ))
       (casting/integer-type? item-dtype)
       (let [item (long item)]
         (reify LongReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (readLong [rdr idx] item)
           (subBuffer [rdr sidx eidx]
             (ChunkedList/sublistCheck sidx eidx n-elems)
             (const-reader item (- eidx sidx)))
           (longReduction [this rfn init]
             (loop [init init
                    idx 0]
               (if (and (< idx n-elems) (not (reduced? init)))
                 (recur (.invokePrim rfn init item) (unchecked-inc idx))
                 init)))
           dtype-proto/PElemwiseReaderCast
           (elemwise-reader-cast [rdr new-dtype] rdr)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)
           dtype-proto/PRangeConvertible
           (convertible-to-range? [this] (== 1 n-elems))
           (->range [this options] (hamf/range item (unchecked-inc item)))
))
       (casting/float-type? item-dtype)
       (let [item (double item)]
         (reify DoubleReader
           (elemwiseDatatype [rdr] item-dtype)
           (lsize [rdr] n-elems)
           (readDouble [rdr idx] item)
           (subBuffer [rdr sidx eidx]
             (ChunkedList/sublistCheck sidx eidx n-elems)
             (const-reader item (- eidx sidx)))
           (doubleReduction [this rfn init]
             (loop [init init
                    idx 0]
               (if (and (< idx n-elems) (not (reduced? init)))
                 (recur (.invokePrim rfn init item) (unchecked-inc idx))
                 init)))
           dtype-proto/PElemwiseReaderCast
           (elemwise-reader-cast [rdr new-dtype] rdr)
           dtype-proto/PConstantTimeMinMax
           (has-constant-time-min-max? [this] true)
           (constant-time-min [this] item)
           (constant-time-max [this] item)
           dtype-proto/PRangeConvertible
           (convertible-to-range? [this] (== 1 n-elems))
           (->range [this options] (hamf/range item (+ item 1.0)))))
       :else
       (reify ObjectReader
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (readObject [rdr _idx] item)
         (subBuffer [rdr sidx eidx]
           (ChunkedList/sublistCheck sidx eidx n-elems)
           (const-reader item (- eidx sidx)))
         (reduce [this rfn init]
             (loop [init init
                    idx 0]
               (if (and (< idx n-elems) (not (reduced? init)))
                 (recur (rfn init item) (unchecked-inc idx))
                 init)))
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
