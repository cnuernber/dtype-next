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
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.casting :as casting])
  (:import [tech.v3.datatype PrimitiveIO LongReader]
           [tech.v3.datatype.monotonic_range Int64Range]
           [clojure.lang IObj MapEntry]
           [java.util Map]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- base-dimension->reverse-long-map
  "This could be expensive in a lot of situations.  Hopefully the sequence is a range.
  We return a map that does the sparse reverse mapping on get.  We can return a number
  or a sequence.  The fallback is (argops/arggroup-by identity dim)"
  ^Map [dim]
  (cond
    (number? dim)
    (let [dim (long dim)]
      (reify Map
        (size [m] (unchecked-int dim))
        (containsKey [m arg]
          (and arg
               (casting/integer-type?
                (dtype-proto/elemwise-datatype arg))
               (let [arg (long arg)]
                 (and (>= arg 0)
                      (< arg dim)))))
        (isEmpty [m] (== dim 0))
        (entrySet [m]
          (->> (range dim)
               (map-indexed (fn [idx range-val]
                              (MapEntry. range-val idx)))
               set))
        (getOrDefault [m k default-value]
          (if (and k (casting/integer-type? (dtype-proto/elemwise-datatype k)))
            (let [arg (long k)]
              (if (and (>= arg 0)
                       (< arg dim))
                [arg]
                default-value))
            default-value))
        (get [m k] [(long k)])))
    (dtype-proto/convertible-to-range? dim)
    (dtype-proto/range->reverse-map (dtype-proto/->range dim {}))
    :else
    (argops/arggroup-by identity (dtype-proto/->reader dim))))


(defn maybe-range-reader
  "Create a range if possible.  If not, return a reader that knows the found mins
  and maxes."
  [^PrimitiveIO reader]
  (let [first-elem (.readLong reader 0)
        second-elem (.readLong reader 1)
        increment (- second-elem first-elem)
        n-elems (.lsize reader)]
    (loop [item-min (min (.readLong reader 0)
                         (.readLong reader 1))
           item-max (max (.readLong reader 0)
                         (.readLong reader 1))
           last-elem (.readLong reader 1)
           constant-increment? true
           idx 2]
      (if (< idx n-elems)
        (let [next-elem (.readLong reader idx)
              next-increment (- next-elem last-elem)
              item-min (min item-min next-elem)
              item-max (max item-max next-elem)]
          (recur item-min item-max next-elem
                 (boolean (and constant-increment?
                               (= next-increment increment)))
                 (unchecked-inc idx)))
        ;;Make a range if we can but if we cannot then at least maintain
        ;;constant min/max behavior
        (if constant-increment?
          (dtype-range/make-range (.readLong reader 0)
                                  (+ last-elem increment) increment)
          (reify
            LongReader
            (lsize [rdr] n-elems)
            (readLong [rdr idx] (.readLong reader idx))
            dtype-proto/PConstantTimeMinMax
            (has-constant-time-min-max? [item] true)
            (constant-time-min [item] item-min)
            (constant-time-max [item] item-max)))))))


(defn- simplify-reader
  [^PrimitiveIO reader]
  (let [n-elems (.lsize reader)]
    (cond
      (= n-elems 1)
      (dtype-range/make-range (.readLong reader 0) (inc (.readLong reader 0)))
      (= n-elems 2)
      (let [start (.readLong reader 0)
            last-elem (.readLong reader 1)
            increment (- last-elem start)]
        (dtype-range/make-range start (+ last-elem increment) increment))
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
  (get-n-repetitions [item])
  (reverse-index-map [item]
    "Return a map implementation that maps indexes from local-space
back into Y global space indexes."))


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

(deftype IndexAlg [^PrimitiveIO reader ^long n-reader-elems
                   ^long offset ^long repetitions]
  LongReader
  (lsize [rdr] (* n-reader-elems repetitions))
  (readLong [rdr idx] (.readLong reader (rem (+ idx offset)
                                             n-reader-elems)))
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item]
    (and (dtype-proto/convertible-to-range? reader)
         (simple? item)))
  (->range [item options] (dtype-proto/->range reader options))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [item]
    (dtype-proto/has-constant-time-min-max? reader))
  (constant-time-min [item] (dtype-proto/constant-time-min reader))
  (constant-time-max [item] (dtype-proto/constant-time-max reader))
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
  (offset? [item] (not= 0 offset))
  (broadcast? [item] (not= 1 repetitions))
  (simple? [item] (and (== 0 offset)
                       (== 1 repetitions)))
  (get-offset [item] offset)
  (get-reader [item] (simplify-range->direct reader))
  (get-n-repetitions [item] repetitions)
  (reverse-index-map [item]
    (let [^Map src-map (reverse-index-map reader)]
      (reify Map
        (size [m] (.size src-map))
        (entrySet [m]
          (->> (.entrySet src-map)
               (map (fn [[k v]]
                      (MapEntry.
                       k
                       (fixup-reverse-v v n-reader-elems
                                        offset repetitions))))
               set))
        (getOrDefault [m k default]
          (let [retval (.getOrDefault src-map k default)]
            (if (identical? retval default)
              default
              (fixup-reverse-v retval n-reader-elems offset repetitions))))
        (get [m k]
          (when-let [v (.get src-map k)]
            (fixup-reverse-v v n-reader-elems offset repetitions))))))
  dtype-proto/PBuffer
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
  ^PrimitiveIO [item-seq]
  ;;Normalize this to account for single digit numbers
  (cond
    (number? item-seq)
    (with-meta
      (dtype-range/make-range (long item-seq))
      {:scalar? true})
    (dtype-proto/convertible-to-range? item-seq)
    (dtype-proto/->range item-seq {})
    (instance? IndexAlg item-seq)
    item-seq
    (dtype-proto/convertible-to-bitmap? item-seq)
    (bitmap/bitmap->efficient-random-access-reader item-seq)
    :else
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
        reader))))


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
  (if (= select-arg :all)
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
         (let [^PrimitiveIO select-arg (if (= select-arg :lla)
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
  (get-n-repetitions [item] 1)
  (reverse-index-map [item]
    (base-dimension->reverse-long-map item)))
