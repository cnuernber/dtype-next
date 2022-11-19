(ns tech.v3.datatype.index-algebra
  "Operations on sets of indexes.  And index set is always representable by a long
  reader.  Indexes that are monotonically incrementing are special as they map to an
  underlying layer with the identity function enabling block transfers or operations
  against the data.  So it is imporant to classify distinct types of indexing
  operations."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.bitmap :as bitmap]
            [tech.v3.datatype.monotonic-range :as dtype-range]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.io-indexed-buffer :as indexed-rdr]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [ham-fisted.api :as hamf])
  (:import [tech.v3.datatype Buffer LongReader]
           [clojure.lang MapEntry]
           [java.util Map]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn maybe-range-reader
  "Create a range if possible.  If not, return a reader that knows the found mins
  and maxes."
  [data]
  (hamf/reduce-reducer (unary-pred/index-reducer :int64) data))


(defn- simplify-reader
  "Ranges tell us a lot more about the data so if we can we like to make
  ranges."
  [^Buffer reader]
  (let [n-elems (.lsize reader)]
    (cond
      (= n-elems 1)
      (hamf/range (.readLong reader 0) (inc (.readLong reader 0)))
      (= n-elems 2)
      (let [start (.readLong reader 0)
            last-elem (.readLong reader 1)
            increment (- last-elem start)]
        (hamf/range start (+ last-elem increment) increment))
      (<= n-elems 5)
      (maybe-range-reader reader)
      :else
      reader)))


(defn simplify-range->direct
  "Ranges starting at 0 and incrementing by 1 can be represented by numbers"
  [item]
  (if (dtype-proto/convertible-to-range? item)
    (let [item-rng (dtype-proto/->range item {})]
      (if (and (== 0 (long (dtype-proto/range-start item-rng)))
               (== 1 (long (dtype-proto/range-increment item-rng))))
        (dtype-proto/ecount item)
        item))
    item))


(defprotocol PIndexAlgebra
  (offset [item offset])
  (broadcast [item n-elems]
    "Make this item larger or smaller by simple duplication of indexes.")
  (offset? [item])
  (broadcast? [item])
  (simple? [item])
  (get-offset [item])
  (get-reader [item])
  (get-n-repetitions [item]))


(defn- fixup-reverse-v
  "Project a v into the index obj space"
  [v ^long n-reader-elems ^long offset ^long repetitions]
  (let [v (if (number? v)
            [v]
            v)
        v (if (not= 0 offset)
            (map #(rem (+ offset (long %))
                       n-reader-elems) v)
            v)
        v (if (not= 1 repetitions)
            (->> (range repetitions)
                 (mapcat (fn [^long rep-idx]
                           (let [offset (* rep-idx n-reader-elems)]
                             (map #(+ offset (long %)) v)))))
            v)]
    v))

(declare ->idx-alg)

(deftype IndexAlg [^Buffer reader ^long n-reader-elems
                   ^long offset ^long repetitions]
  LongReader
  (lsize [_rdr] (* n-reader-elems repetitions))
  (readLong [_rdr idx] (.readLong reader (rem (+ idx offset)
                                             n-reader-elems)))
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item]
    (and (dtype-proto/convertible-to-range? reader)
         (simple? item)))
  (->range [_item options] (dtype-proto/->range reader options))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [_item]
    (dtype-proto/has-constant-time-min-max? reader))
  (constant-time-min [_item] (dtype-proto/constant-time-min reader))
  (constant-time-max [_item] (dtype-proto/constant-time-max reader))
  PIndexAlgebra
  (offset [item new-offset]
    (let [new-offset (rem (+ offset (long new-offset))
                          n-reader-elems)
          new-offset (long (if (< new-offset 0)
                             (+ new-offset n-reader-elems)
                             new-offset))]
      (if (and (== 0 new-offset)
               (== 1 repetitions))
        (.get-reader item)
        (IndexAlg. reader n-reader-elems
                   new-offset
                   repetitions))))
  (broadcast [item n-elems]
    (let [n-elems (long n-elems)]
      (when-not (== 0 (rem n-elems n-reader-elems))
        (throw (Exception.
                (format "Requested length %s and base length %s are not commensurate"
                        n-elems n-reader-elems))))
      (let [new-reps (quot n-elems n-reader-elems)]
        (if (and (== 0 offset)
                 (== 1 new-reps))
          (.get-reader item)
          (IndexAlg. reader n-reader-elems
                     offset
                     new-reps)))))
  (offset? [_item] (not= 0 offset))
  (broadcast? [_item] (not= 1 repetitions))
  (simple? [_item] (and (== 0 offset)
                       (== 1 repetitions)))
  (get-offset [_item] offset)
  (get-reader [_item] (simplify-range->direct reader))
  (get-n-repetitions [_item] repetitions)
  dtype-proto/PSubBuffer
  (sub-buffer [item new-offset len]
    (let [new-offset (long new-offset)
          len (long len)]
      (when-not (<= (+ new-offset len) (.lsize item))
        (throw (Exception. "Sub buffer out of range.")))
      (when-not (> len 0)
        (throw (Exception. "Length is not > 0")))
      (let [rel-offset (rem (long (+ offset new-offset))
                            n-reader-elems)
            len (long len)]
        (if (<= (+ rel-offset len) n-reader-elems)
          (dtype-proto/sub-buffer reader rel-offset len)
          (reify LongReader
            (lsize [rdr] len)
            (readLong [rdr idx]
              (.readLong item (+ idx new-offset)))))))))



(defn dimension->reader
  "Given a generic thing, make the appropriate longreader that is geared towards rapid random access
  and, when possible, has constant time min/max operations."
  ^Buffer [item-seq]
  ;;Normalize this to account for single digit numbers
  (cond
    (number? item-seq)
    (dimension->reader
     (with-meta
       (dtype-range/make-range (long item-seq))
       {:scalar? true}))
    (dtype-proto/convertible-to-range? item-seq)
    (-> (dtype-proto/->range item-seq {})
        (dtype-proto/->reader))
    (instance? IndexAlg item-seq)
    item-seq
    (dtype-proto/convertible-to-bitmap? item-seq)
    (dtype-proto/->reader (bitmap/->random-access item-seq))
    :else
    (->
     (let [item-seq (if (dtype-proto/convertible-to-reader? item-seq)
                      item-seq
                      (long-array item-seq))
           n-elems (dtype-base/ecount item-seq)
           reader (dtype-base/->reader item-seq)]
       (cond
         (= n-elems 1) (dtype-range/make-range (.readLong reader 0)
                                               (inc (.readLong reader 0)))
         (= n-elems 2)
         (let [start (.readLong reader 0)
               last-elem (.readLong reader 1)
               increment (- last-elem start)]
           (dtype-range/make-range start (+ last-elem increment) increment))
         ;;Try to catch quick,hand made ranges out of persistent vectors and such.
         (<= n-elems 5)
         (maybe-range-reader reader)
         :else
         reader))
     (dtype-proto/->reader))))


(defn ->index-alg
  ^IndexAlg [data]
  (if (instance? IndexAlg data)
    data
    (let [rdr (dimension->reader data)]
      (IndexAlg. rdr (.lsize rdr) 0 1))))


(defn select
  "Given a 'dimension', select and return a new 'dimension'.  A dimension could be
  a number which implies the range 0->number else it could be something convertible
  to a long reader.  This algorithm attempts to aggresively minimize the complexity
  of the returned dimension object.
  arguments may be
  :all - no change
  :lla - reverse index
  :number - select that index
  :sequence - select items in sequence.  Ranges will be better supported than truly
  random access."
  [dim select-arg]
  (if (identical? select-arg :all)
    dim
    (let [rdr (dimension->reader dim)
          n-elems (.lsize rdr)]
      (if (number? select-arg)
        (let [select-arg (long select-arg)]
          (when-not (< select-arg (.lsize rdr))
            (throw (Exception. (format "Index out of range: %s >= %s"
                                       select-arg (.lsize rdr)))))
          (let [read-value (.readLong rdr select-arg)]
            (with-meta
              (dtype-range/make-range read-value (unchecked-inc read-value))
              {:select-scalar? true})))
        (simplify-range->direct
         (let [^Buffer select-arg (if (= select-arg :lla)
                                    (dtype-range/reverse-range n-elems)
                                    (dimension->reader select-arg))
               n-select-arg (.lsize select-arg)]
           (if (dtype-proto/convertible-to-range? select-arg)
             (let [select-arg (dtype-proto/->range select-arg {})]
               (if (dtype-proto/convertible-to-range? dim)
                 (dtype-proto/range-select (dtype-proto/->range dim {})
                                           select-arg)
                 (let [sel-arg-start (long (dtype-proto/range-start select-arg))
                       sel-arg-increment (long (dtype-proto/range-increment
                                                select-arg))
                       select-arg (dtype-proto/range-offset select-arg
                                                            (- sel-arg-start))
                       ;;the sub buffer operation here has a good chance of simplifying
                       ;;the dimension object.
                       rdr (dtype-proto/sub-buffer rdr
                                                   sel-arg-start
                                                   (* sel-arg-increment n-select-arg))]
                   (simplify-reader
                    (if (== 1 (long (dtype-proto/range-increment select-arg)))
                      rdr
                      (indexed-rdr/indexed-buffer select-arg rdr))))))
             (simplify-reader
              (indexed-rdr/indexed-buffer select-arg rdr)))))))))


(defn dense?
  "Are the indexes packed, increasing or decreasing by one."
  [dim]
  (boolean
   (or (number? dim)
       (and (dtype-proto/convertible-to-range? dim)
            (== 1 (long (dtype-proto/range-increment
                         (dtype-proto/->range dim {}))))))))


(defn direct?
  "Is the data represented natively, indexes starting at zero and incrementing by
  one?"
  [dim]
  (number? dim))


(defn direct-reader?
  [dim]
  (or (number? dim)
      (number? (get-reader dim))))


(extend-type Object
  PIndexAlgebra
  (offset [item offset-val]
    (if (== 0 (long offset-val))
      item
      (offset (->index-alg item) offset-val)))
  (broadcast [item n-elems]
    (let [item-ecount (if (number? item)
                        (long item)
                        (dtype-base/ecount item))]
      (if (== (long n-elems) item-ecount)
        item
        (broadcast (->index-alg item) n-elems))))
  (offset? [item] false)
  (get-offset [item] 0)
  (broadcast? [item] false)
  (simple? [item] true)
  (get-offset [item] 0)
  (get-reader [item] item)
  (get-n-repetitions [item] 1))
