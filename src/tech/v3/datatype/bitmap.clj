(ns tech.v3.datatype.bitmap
  "Functions for working with RoaringBitmaps.  These are integrated deeply into
  several tech.v3.datatype algorithms and have many potential applications in
  high performance computing applications as they are both extremely fast and
  storage-space efficient."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.clj-range :as clj-range]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.array-buffer]
            [ham-fisted.api :as hamf]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.set :as set])
  (:import [org.roaringbitmap RoaringBitmap IntConsumer]
           [tech.v3.datatype SimpleLongSet LongReader LongBitmapIter Buffer]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [clojure.lang LongRange IFn$OLO IFn$ODO IDeref]
           [java.lang.reflect Field]))


(set! *warn-on-reflection* true)


(def ^{:private true} int-array-class (Class/forName "[I"))


(defn- ensure-int-array
  ^ints [item]
  (when-not (instance? int-array-class item)
    (let [ary-buf (dtype-cmc/make-container :uint32 item)]
      (.ary-data ^ArrayBuffer (dtype-proto/->array-buffer ary-buf)))))


(defn- reduce-into-bitmap
  ^RoaringBitmap [data]
  (set/unique {:set-constructor #(RoaringBitmap.)} data))


(defn- range->bitmap
  [item]
  (let [r (dtype-proto/->range item {})
        rstart (long (dtype-proto/range-start item))
        rinc (long (dtype-proto/range-increment item))
        rend (+ rstart (* rinc (dtype-base/ecount item)))]
    (cond
      (== rstart rend)
      (RoaringBitmap.)
      (== 1 rinc)
      (doto (RoaringBitmap.)
        (.add rstart rend))
      :else
      (reduce-into-bitmap r))))


(declare ->bitmap)


(defn as-range
  "If this is convertible to a long range, then return a range else return nil."
  [bm]
  (when-let [^RoaringBitmap bm (dtype-proto/as-roaring-bitmap bm)]
    (if (.isEmpty bm)
      (hamf/range 0)
      (let [start (Integer/toUnsignedLong (.first bm))
            end (unchecked-inc (Integer/toUnsignedLong (.last bm)))]
        (when (.contains bm start end)
          (hamf/range start end))))))


(defn ->random-access
  "Bitmaps do not implement efficient random access although we do provide access of
  inefficient random access for them.  This converts a bitmap into a flat buffer of data
  that does support efficient random access."
  [bitmap]
  (set/->integer-random-access bitmap))


(deftype IntReduceConsumer [^:unsynchronized-mutable acc
                            ^IFn$OLO rfn]
  IntConsumer
  (accept [this v]
    (when-not (reduced? acc)
      (set! acc (.invokePrim rfn acc (Integer/toUnsignedLong v)))))
  IDeref
  (deref [this] acc))


(extend-type RoaringBitmap
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [bitmap] :uint32)
  dtype-proto/PDatatype
  (datatype [bitmap] :datatype)
  dtype-proto/PECount
  (ecount [bitmap] (.getLongCardinality bitmap))
  dtype-proto/PToReader
  (convertible-to-reader? [bitmap] true)
  (->reader [bitmap] (->random-access bitmap))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [bitmap] (not (.isEmpty bitmap)))
  (constant-time-min [bitmap] (Integer/toUnsignedLong (.first bitmap)))
  (constant-time-max [bitmap] (Integer/toUnsignedLong (.last bitmap)))
  dtype-proto/PRangeConvertible
  (convertible-to-range? [item] (or (.isEmpty item)
                                    (.contains item
                                               (Integer/toUnsignedLong (.first item))
                                               (Integer/toUnsignedLong (.last item)))))
  (->range [item options] (as-range item))
  dtype-proto/PClone
  (clone [bitmap] (.clone bitmap))
  dtype-proto/PToBitmap
  (convertible-to-bitmap? [item] true)
  (as-roaring-bitmap [item] item)
  hamf-proto/PAdd
  (add-fn [lhs] (hamf/long-accumulator
                 acc v (.add ^RoaringBitmap acc (unchecked-int v)) acc))
  hamf-proto/SetOps
  (set? [lhs] true)
  (intersection [lhs rhs] (RoaringBitmap/and lhs (->bitmap rhs)))
  (difference [lhs rhs] (RoaringBitmap/andNot lhs (->bitmap rhs)))
  (union [lhs rhs] (RoaringBitmap/or lhs (->bitmap rhs)))
  (xor [lhs rhs] (RoaringBitmap/xor lhs (->bitmap rhs)))
  (contains-fn [lhs] (hamf/long-predicate v (.contains lhs (unchecked-int v))))
  (cardinality [lhs] (.getCardinality lhs))
  hamf-proto/BitSet
  (bitset? [lhs] true)
  (contains-range? [lhs sidx eidx]
    (let [sidx (long sidx)
          eidx (long eidx)]
      (if (< sidx 0) false
          (.contains lhs sidx eidx))))
  (intersects-range? [lhs sidx eidx]
    (let [sidx (max 0 (long sidx))
          eidx (max 0 (long eidx))]
      (if (== sidx eidx)
        false
        (.intersects lhs sidx eidx))))
  (min-set-value [lhs] (Integer/toUnsignedLong (.first lhs)))
  (max-set-value [lhs] (Integer/toUnsignedLong (.last lhs)))
  hamf-proto/Reduction
  (reducible? [this] true)
  (reduce [coll rfn acc]
    (if-let [r (as-range coll)]
      (hamf/reduce r rfn acc)
      (let [^IFn$OLO rfn (cond
                           (instance? IFn$OLO rfn)
                           rfn
                           (instance? IFn$ODO rfn)
                           (hamf/long-accumulator
                            acc v
                            (.invokePrim ^IFn$ODO rfn acc v))
                           :else
                           (hamf/long-accumulator
                            acc v (rfn acc v)))
            c (IntReduceConsumer. acc rfn)]
        (when-not (reduced? acc)
          (.forEach coll c))
        @c))))


(dtype-pp/implement-tostring-print RoaringBitmap)


(casting/add-object-datatype! :bitmap RoaringBitmap false)


(defn ->bitmap
  "Create a roaring bitmap.  If this object has a conversion to a roaring bitmap use
  that, else copy the data into a new roaring bitmap."
  (^RoaringBitmap [item]
   (cond
     (nil? item)
     (RoaringBitmap.)
     (dtype-proto/convertible-to-bitmap? item)
     (dtype-proto/as-roaring-bitmap item)
     (dtype-proto/convertible-to-range? item)
     (range->bitmap (dtype-proto/->range item nil))
     :else
     (reduce-into-bitmap item)))
  (^RoaringBitmap []
   (RoaringBitmap.))
  (^RoaringBitmap [^long sidx ^long eidx]
   (range->bitmap (hamf/range sidx eidx))))


(defn offset
  "Offset a bitmap creating a new bitmap."
  ^RoaringBitmap[^RoaringBitmap bm ^long offset]
  (RoaringBitmap/addOffset bm (unchecked-int offset)))


(defn ->unique-bitmap
  "Perform a conversion to a bitmap.  If this thing is already a bitmap, clone it."
  (^RoaringBitmap [item]
   (if (dtype-proto/convertible-to-bitmap? item)
     (.clone ^RoaringBitmap (dtype-proto/as-roaring-bitmap item))
     (->bitmap item)))
  (^RoaringBitmap []
   (RoaringBitmap.)))


(defn reduce-union
  ^RoaringBitmap [bitmaps]
  (hamf/reduce (fn [lhs rhs]
                 (.or ^RoaringBitmap lhs rhs)
                 lhs)
               (RoaringBitmap.)
               bitmaps))

(defn reduce-intersection
  ^RoaringBitmap [bitmaps]
  (hamf/reduce (fn [lhs rhs]
                 (.and ^RoaringBitmap lhs rhs)
                 lhs)
               (RoaringBitmap.)
               bitmaps))
