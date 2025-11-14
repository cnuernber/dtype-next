(ns tech.v3.datatype.unary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :as double-ops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.monotonic-range :as mono-range]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.defprotocol :refer [extend extend-type extend-protocol]])
  (:import [tech.v3.datatype UnaryPredicate Buffer
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$LongUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate
            Buffer BooleanReader IndexConsumer]
           [org.roaringbitmap RoaringBitmap]
           [ham_fisted Casts Ranges$LongRange IMutList Reducible]
           [java.util List]
           [java.util.function DoublePredicate Predicate LongConsumer]
           [clojure.lang IFn IDeref])
  (:refer-clojure :exclude [extend extend-type extend-protocol]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn keyword-predicate
  ^UnaryPredicates$ObjectUnaryPredicate [kwd]
  (reify UnaryPredicates$ObjectUnaryPredicate
    (unaryObject [this arg]
      (identical? kwd arg))))


(defn ifn->unary-predicate
  (^UnaryPredicate [ifn opname]
   (when-not (instance? IFn ifn)
     (throw (Exception. (format "Arg (%s) is not an instance of IFn"
                                ifn))))
   (reify
     UnaryPredicates$ObjectUnaryPredicate
     (unaryObject [this arg]
       (Casts/booleanCast (ifn arg)))
     dtype-proto/POperator
     (op-name [this] opname)))
  (^UnaryPredicate [ifn]
   (ifn->unary-predicate ifn :_unnamed)))


(defn ->predicate
  (^UnaryPredicate [item opname]
   (cond
     (instance? UnaryPredicate item) item
     (keyword? item) (keyword-predicate item)
     (instance? IFn item) (ifn->unary-predicate item opname)
     (instance? DoublePredicate item)
     (let [^DoublePredicate item item]
       (reify
         UnaryPredicates$DoubleUnaryPredicate
         (unaryDouble [this arg]
           (.test item arg))
         dtype-proto/POperator
         (op-name [this] opname)))
     (instance? Predicate item)
     (let [^Predicate item item]
       (reify
         UnaryPredicates$ObjectUnaryPredicate
         (unaryObject [this arg]
           (.test item arg))
         dtype-proto/POperator
         (op-name [this] opname)))
     (instance? Comparable item)
     (reify
       UnaryPredicates$ObjectUnaryPredicate
       (unaryObject [this arg]
         (== 0 (.compareTo ^Comparable item arg)))
       dtype-proto/POperator
       (op-name [this] opname))
     :else
     (reify
       UnaryPredicates$ObjectUnaryPredicate
       (unaryObject [this arg]
         (boolean (= item arg)))
       dtype-proto/POperator
       (op-name [this] opname))))
  (^UnaryPredicate [item]
   (->predicate item :_unnamed)))


(defn iterable
  [pred src]
  (let [pred (->predicate pred)
        src (dtype-base/->iterable src)]
    (dispatch/typed-map-1 pred :boolean src)))


(defn reader
  ^Buffer [pred src-rdr]
  (let [pred (->predicate pred)
        op-space (casting/simple-operation-space
                  (packing/unpack-datatype
                   (dtype-base/operational-elemwise-datatype src-rdr)))
        src-rdr (dtype-base/->reader src-rdr op-space)]
    (case op-space
      :int64
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readObject [rdr idx]
          (.unaryLong pred (.readLong src-rdr idx))))
      :float64
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readObject [rdr idx]
          (.unaryDouble pred (.readDouble src-rdr idx))))
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readObject [rdr idx]
          (.unaryObject pred (.readObject src-rdr idx)))))))


(defn ifn->long-unary-predicate
  (^UnaryPredicate [ifn opname]
   (when-not (instance? IFn ifn)
     (throw (Exception. (format "Arg (%s) is not an instance of IFn"
                                ifn))))
   (reify
     UnaryPredicates$ObjectUnaryPredicate
     (unaryObject [this arg]
       (boolean (ifn arg)))
     dtype-proto/POperator
     (op-name [this] opname)))
  (^UnaryPredicate [ifn]
   (ifn->unary-predicate ifn :_unnamed)))


(def builtin-ops
  {:tech.numerics/not
   (reify
     UnaryPredicates$ObjectUnaryPredicate
     (unaryObject [this arg]
       (if (Casts/booleanCast arg) false true))
     dtype-proto/POperator
     (op-name [this] :not))
   :tech.numerics/nan?
   (vary-meta (reify
                UnaryPredicates$DoubleUnaryPredicate
                (unaryDouble [this arg]
                  (Double/isNaN arg))
                dtype-proto/POperator
                (op-name [this] :nan?))
              assoc :operation-space :float32)
   :tech.numerics/finite?
   (vary-meta (reify
                UnaryPredicates$DoubleUnaryPredicate
                (unaryDouble [this arg]
                  (Double/isFinite arg))
                dtype-proto/POperator
                (op-name [this] :finite?))
              assoc :operation-space :float32)
   :tech.numerics/infinite?
   (vary-meta (reify
                UnaryPredicates$DoubleUnaryPredicate
                (unaryDouble [this arg]
                  (Double/isInfinite arg))
                dtype-proto/POperator
                (op-name [this] :infinite?))
              assoc :operation-space :float32)
   :tech.numerics/mathematical-integer?
   (vary-meta (reify
                UnaryPredicates$DoubleUnaryPredicate
                (unaryDouble [this arg]
                  (double-ops/is-mathematical-integer? arg))
                dtype-proto/POperator
                (op-name [this] :mathematical-integer?))
              assoc :operation-space :float32)
   :tech.numerics/pos?
   (reify
     UnaryPredicates$ObjectUnaryPredicate
     (unaryLong [this arg]
       (pos? arg))
     (unaryDouble [this arg]
       (pos? arg))
     (unaryObject [this arg]
       (pos? (double arg)))
     dtype-proto/POperator
     (op-name [this] :pos?))
   :tech.numerics/neg?
   (reify
     UnaryPredicates$ObjectUnaryPredicate
     (unaryLong [this arg]
       (neg? arg))
     (unaryDouble [this arg]
       (neg? arg))
     (unaryObject [this arg]
       (neg? (double arg)))
     dtype-proto/POperator
     (op-name [this] :neg?))
   :tech.numerics/even? (ifn->long-unary-predicate even? :even?)
   :tech.numerics/odd? (ifn->long-unary-predicate odd? :odd?)
   :tech.numerics/zero?
   (reify
     UnaryPredicates$ObjectUnaryPredicate
     (unaryLong [this arg]
       (zero? arg))
     (unaryDouble [this arg]
       (zero? arg))
     (unaryObject [this arg]
       (zero? (double arg)))
     dtype-proto/POperator
     (op-name [this] :zero?))})


(defn reader-index-space
  [rdr]
  (if (< (dtype-base/ecount rdr) Integer/MAX_VALUE)
    :int32
    :int64))

(deftype IndexList [^IMutList list
                    ^{:unsynchronized-mutable true
                      :tag long} first-value
                    ^{:unsynchronized-mutable true
                      :tag long} last-value
                    ^{:unsynchronized-mutable true
                      :tag long} increment
                    ^{:unsynchronized-mutable true
                      :tag long} min-value
                    ^{:unsynchronized-mutable true
                      :tag long} max-value]
  LongConsumer
  (accept [_this lval]
    (if (== first-value Long/MIN_VALUE)
      (set! first-value lval)
      (let [new-incr (- lval last-value)]
        (cond
          (== increment Long/MAX_VALUE)
          (.addLong list lval)
          (== increment new-incr) nil
          (and (not (== new-incr 0)) (== increment Long/MIN_VALUE))
          (set! increment new-incr)
          (or (== new-incr 0) (not (== increment new-incr)))
          (do (if (== increment Long/MIN_VALUE)
                (.addLong list first-value)
                (.addAll list (hamf/range first-value (+ last-value increment) increment)))
              (.addLong list lval)
              (set! increment Long/MAX_VALUE)))))
    (set! last-value lval)
    (set! min-value (min lval min-value))
    (set! max-value (max lval max-value)))
  Reducible
  (reduce [this other]
    (cond
      (== first-value Long/MIN_VALUE) other
      (== (.-first-value ^IndexList other) Long/MIN_VALUE) this
      :else
      (let [^IndexList other other
            new-min (min min-value (.-min-value other))
            new-max (max max-value (.-max-value other))]
        (if (and (not (== increment Long/MAX_VALUE))
                 (== increment (.-increment other))
                 (== (+ last-value increment) (.-first-value other)))
          (IndexList. list first-value (.-last-value other) increment min-value max-value)
          (let [^IMutList list list]
            (when (not (== increment Long/MAX_VALUE))
              (.addAll list (hamf/range first-value (+ last-value increment) increment)))
            (if (== (.-increment other) Long/MAX_VALUE)
              (.addAll list (.-list other))
              (.addAll list (hamf/range (.-first-value other)
                                        (+ (.-last-value other)
                                           (.-increment other))
                                        (.-increment other))))
            (IndexList. list first-value (list -1) Long/MAX_VALUE min-value max-value))))))
  IDeref
  (deref [_this]
    (vary-meta
     (cond
       (== first-value Long/MIN_VALUE)
       (hamf/range 0)
       (== increment Long/MAX_VALUE)
       list
       :else
       (hamf/range first-value (+ last-value increment) increment))
     assoc :min min-value :max max-value)))


(defn index-reducer
  "Return a hamf parallel reducer that reduces indexes into an int32 space,
  a int64 space, or uses a roaring bitmap.

  * dtype - :int32 (default), :int64, :bitmap.  If dtype is a number, then
  if it is less than int max the space is :int32 else :int64"
  [dtype]
  (let [dtype (if (number? dtype)
                (if (< (long dtype) Integer/MAX_VALUE)
                  :int32
                  :int64)
                (or dtype :int32))]
    (cond
      (or (identical? dtype :int32) (identical? dtype :int64))
      (reify
        hamf-proto/Reducer
        (->init-val-fn [r] #(IndexConsumer. hamf/range (dtype-list/make-list dtype)))
        (->rfn [r] hamf-rf/long-consumer-accumulator)
        hamf-proto/Finalize
        (finalize [r l] @l)
        hamf-proto/ParallelReducer
        (->merge-fn [r] hamf-rf/reducible-merge))
      (identical? dtype :bitmap)
      (reify
        hamf-proto/Reducer
        (->init-val-fn [r] #(RoaringBitmap.))
        (->rfn [r] (hamf-rf/long-accumulator
                    acc v
                    (.add ^RoaringBitmap acc (unchecked-int v))
                    acc))
        hamf-proto/Finalize
        (finalize [r l] l)
        hamf-proto/ParallelReducer
        (->merge-fn [r] (fn [^RoaringBitmap l ^RoaringBitmap r]
                          (.or l r)
                          l)))
      :else
      (throw (Exception. "Unrecognized index reducer type.")))))


(defn bool-reader->indexes
  "Given a reader, produce a filtered list of indexes filtering out 'false' values."
  (^Buffer [{:keys [storage-type] :as _options} x]
   (let [n-elems (dtype-base/ecount x)
         reader (dtype-base/->reader x)
         storage-type (or storage-type (reader-index-space x))]
     (->> (hamf/range n-elems)
          (lznc/filter (hamf-fn/long-predicate
                        idx
                        (Casts/booleanCast (.readObject reader idx))))
          (hamf-rf/preduce-reducer (index-reducer storage-type)
                                {:ordered? true}))))
  (^Buffer [x]
   (bool-reader->indexes nil x)))
