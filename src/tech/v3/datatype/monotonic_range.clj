(ns tech.v3.datatype.monotonic-range
  "Ranges that *are* readers.  And that support some level of algebraic operations
  between pairs of them."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.hamf-proto :as hamf-proto]
            [tech.v3.datatype.casting :as casting]
            ;;Complete clojure range support
            [tech.v3.datatype.clj-range :as clj-range]
            [tech.v3.datatype.errors :as errors]
            [ham-fisted.api :as hamf])
  (:import [tech.v3.datatype LongReader DoubleReader]
           [clojure.lang LongRange IObj IPersistentMap Range MapEntry]
           [java.lang.reflect Field]
           [java.util Map]
           [ham_fisted Ranges Ranges$LongRange Ranges$DoubleRange]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare make-range)


(extend-type Ranges$LongRange
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [_item] :int64)
  dtype-proto/PECount
  (ecount [item] (.-nElems item))
  dtype-proto/PSubBuffer
  (sub-buffer [item offset len]
    (.subList item (long offset) (long len)))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [item] (not (== 0 (.-nElems item))))
  (constant-time-min [item]
    (if (pos? (.-step item))
      (.-start item)
      (item -1)))
  (constant-time-max [item]
    (if (pos? (.-step item))
      (item -1)
      (.-start item)))
  dtype-proto/PRangeConvertible
  (convertible-to-range? [_item] true)
  (->range [item _options] item)
  dtype-proto/PClone
  (clone [_this] _this)
  dtype-proto/PRange
  (range-select [lhs rhs]
    (let [r-start (long (dtype-proto/range-start rhs))
          r-n-elems (long (dtype-proto/ecount rhs))
          r-inc (long (dtype-proto/range-increment rhs))
          start (.-start lhs)
          increment (.-step lhs)
          n-elems (.-nElems lhs)
          ;;As start is included in n-elems, (* inc (dec n-elems))
          ;;is stop.
          r-stop (+ r-start (* (dec r-n-elems) r-inc))
          new-start (+ start (* r-start increment))
          new-inc (* r-inc increment)]
      (when (or (> r-stop n-elems)
                (>= r-start n-elems))
        (throw (Exception. (format "select-ranges - %s %s - righthand side out of range"
                                   [start increment n-elems]
                                   [r-start r-inc r-n-elems]))))
      (Ranges$LongRange. new-start (+ new-start (* r-n-elems new-inc)) new-inc {})))
  (range-start [item] (.-start item))
  (range-increment [item] (.-step item))
  (range-min [item] (dtype-proto/constant-time-min item))
  (range-max [item] (dtype-proto/constant-time-max item))
  (range-offset [item offset] (Ranges$LongRange. (+ (.-start item) (long offset))
                                                 (+ (.-end item) (long offset))
                                                 (.-step item)
                                                 (meta item)))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] (with-meta
                     (reify
                       LongReader
                       (lsize [b] (.-nElems item))
                       (readLong [b idx] (.lgetLong item idx))
                       (subBuffer [b sidx eidx]
                         (-> (.subList item sidx eidx)
                             (dtype-proto/->buffer)))
                       (reduce [b rfn ival] (.reduce item rfn ival))
                       dtype-proto/PConstantTimeMinMax
                       (has-constant-time-min-max? [b] true)
                       (constant-time-min [b] (dtype-proto/constant-time-min item))
                       (constant-time-max [b] (dtype-proto/constant-time-max item))
                       dtype-proto/PRangeConvertible
                       (convertible-to-range? [b] true)
                       (->range [b _options] item))
                     (meta item)))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item)))


(extend-type Ranges$DoubleRange
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [_item] :float64)
  dtype-proto/PECount
  (ecount [item] (.-nElems item))
  dtype-proto/PSubBuffer
  (sub-buffer [item offset len]
    (.subList item (long offset) (long len)))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [item] (not (== 0 (.-nElems item))))
  (constant-time-min [item]
    (if (pos? (.-step item))
      (.-start item)
      (item -1)))
  (constant-time-max [item]
    (if (pos? (.-step item))
      (item -1)
      (.-start item)))
  dtype-proto/PRangeConvertible
  (convertible-to-range? [_item] true)
  (->range [item _options] item)
  dtype-proto/PClone
  (clone [_this] _this)
  dtype-proto/PRange
  (range-select [lhs rhs]
    (let [r-start (long (dtype-proto/range-start rhs))
          r-n-elems (long (dtype-proto/ecount rhs))
          r-inc (long (dtype-proto/range-increment rhs))
          start (.-start lhs)
          increment (.-step lhs)
          n-elems (.-nElems lhs)
          ;;As start is included in n-elems, (* inc (dec n-elems))
          ;;is stop.
          r-stop (+ r-start (* (dec r-n-elems) r-inc))
          new-start (+ start (* r-start increment))
          new-inc (* r-inc increment)]
      (when (or (> r-stop n-elems)
                (>= r-start n-elems))
        (throw (Exception. (format "select-ranges - %s %s - righthand side out of range"
                                   [start increment n-elems]
                                   [r-start r-inc r-n-elems]))))
      (Ranges$DoubleRange. new-start (+ new-start (* r-n-elems new-inc)) new-inc {})))
  (range-start [item] (.-start item))
  (range-increment [item] (.-step item))
  (range-min [item] (dtype-proto/constant-time-min item))
  (range-max [item] (dtype-proto/constant-time-max item))
  (range-offset [item offset] (Ranges$DoubleRange. (+ (.-start item) (long offset))
                                                   (+ (.-end item) (long offset))
                                                   (.-step item)
                                                   (meta item)))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item]
    (with-meta (reify
                 DoubleReader
                 (lsize [b] (.-nElems item))
                 (readDouble [b idx] (.lgetDouble item idx))
                 (subBuffer [b sidx eidx]
                   (-> (.subList item sidx eidx)
                       (dtype-proto/->buffer)))
                 (reduce [b rfn ival] (.reduce item rfn ival))
                 dtype-proto/PConstantTimeMinMax
                 (has-constant-time-min-max? [b] true)
                 (constant-time-min [b] (dtype-proto/constant-time-min item))
                 (constant-time-max [b] (dtype-proto/constant-time-max item))
                 dtype-proto/PRangeConvertible
                 (convertible-to-range? [b] true)
                 (->range [b _options] item))
      (meta item)))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] (dtype-proto/->buffer item)))


(extend-protocol dtype-proto/PRangeConvertible
  Byte
  (convertible-to-range? [item] true)
  (->range [item options]
    (with-meta (make-range (long item))
      {:scalar? true}))
  Short
  (convertible-to-range? [item] true)
  (->range [item options]
    (with-meta (make-range (long item))
      {:scalar? true}))
  Integer
  (convertible-to-range? [item] true)
  (->range [item options]
    (with-meta (make-range (long item))
      {:scalar? true}))
  Long
  (convertible-to-range? [item] true)
  (->range [item options]
    (with-meta (make-range (long item))
      {:scalar? true})))


(defn make-range
  ([start end increment datatype]
   (when-not (= datatype :int64)
     (throw (RuntimeException. (str "Only :int64 ranges supported for now - " datatype))))
   (let [start (long start)
         end (long end)
         increment (long increment)]
     (when (== 0 increment)
       (throw (Exception. "Infinite range detected - zero increment")))
     (Ranges$LongRange. start end increment {})))
  ([start end increment]
   (make-range start end increment (hamf-proto/elemwise-datatype start)))
  ([start end]
   (make-range start end 1))
  ([end]
   (make-range 0 end 1)))


(extend-type LongRange
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item] true)
  (->range [rng options]
    (let [start (long (first rng))
          step (long (.get ^Field clj-range/lr-step-field rng))
          n-elems (.count rng)]
      (Ranges$LongRange. start (+ start (* n-elems step)) step {}))))


(extend-type Range
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item] (casting/integer-type?
                                 (dtype-proto/elemwise-datatype item)))
  (->range [rng options]
    (when-not (casting/integer-type? (dtype-proto/elemwise-datatype rng))
      (throw (Exception. (format "Item is not convertible to range: %s<%s>"
                                 (type rng) (dtype-proto/elemwise-datatype rng)))))
    (let [start (long (first rng))
          step (long  (.get ^Field clj-range/r-step-field rng))
          n-elems (.count rng)]
      (Ranges$LongRange. start (+ start (* n-elems step)) step {}))))


(defn reverse-range
  ([len]
   (make-range (unchecked-dec (long len)) -1 -1))
  ([start end]
   (make-range (unchecked-dec (long end)) start -1)))
