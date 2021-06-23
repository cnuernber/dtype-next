(ns tech.v3.datatype.unary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :as double-ops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.monotonic-range :as mono-range])
  (:import [tech.v3.datatype UnaryPredicate Buffer
            UnaryPredicates$BooleanUnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$LongUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate
            BooleanReader LongReader DoubleReader ObjectReader
            PrimitiveList]
           [tech.v3.datatype.monotonic_range Int64Range]
           [java.util List]
           [java.util.function DoublePredicate Predicate]
           [clojure.lang IFn IDeref]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(declare keyword-predicate)


(defn ifn->unary-predicate
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
                  (dtype-base/elemwise-datatype src-rdr))
        src-rdr (dtype-base/->reader src-rdr op-space)]
    (case op-space
      :boolean
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryBoolean pred (.readBoolean src-rdr idx))))
      :int64
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryLong pred (.readLong src-rdr idx))))
      :float64
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryDouble pred (.readDouble src-rdr idx))))
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryObject pred (.readObject src-rdr idx)))))))


(defn ifn->long-unary-predicate
  (^UnaryPredicate [ifn opname]
   (when-not (instance? IFn ifn)
     (throw (Exception. (format "Arg (%s) is not an instance of IFn"
                                ifn))))
   (reify
     UnaryPredicates$LongUnaryPredicate
     (unaryObject [this arg]
       (boolean (ifn arg)))
     dtype-proto/POperator
     (op-name [this] opname)))
  (^UnaryPredicate [ifn]
   (ifn->unary-predicate ifn :_unnamed)))


(def builtin-ops
  {:tech.numerics/not
   (reify
     UnaryPredicates$BooleanUnaryPredicate
     (unaryBoolean [this arg]
       (if arg false true))
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
   :tech.numerics/odd? (ifn->long-unary-predicate even? :odd?)
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

(deftype IndexList [^PrimitiveList list
                    ^{:unsynchronized-mutable true
                      :tag long} last-value
                    ^{:unsynchronized-mutable true
                      :tag long} increment]
  PrimitiveList
  (addLong [this lval]
    (when-not (== last-value -1)
      (let [new-incr (- lval last-value)]
        (if (== increment -1)
          (set! increment new-incr)
          (when-not (== new-incr increment)
            (set! increment Long/MAX_VALUE)))))
    (set! last-value lval)
    (.addLong list lval))
  IDeref
  (deref [this]
    (if (== 1 increment)
      (let [lstart (unchecked-long (list 0))]
        (mono-range/make-range lstart (+ lstart (.lsize list))))
      list))
  dtype-proto/PRange
  (range-increment [this] increment))


(defn make-index-list
  ^PrimitiveList [dtype]
  (let [list (dtype-list/make-list dtype)]
    (IndexList. list -1 -1)))


(defn merge-index-list-results
  [index-space lhs rhs]
  (cond
    (== 0 (.size ^List lhs)) rhs
    (== 0 (.size ^List rhs)) lhs
    :else
    (let [^Int64Range lrange (when (instance? Int64Range lhs) lhs)
          ^Int64Range rrange (when (instance? Int64Range rhs) rhs)]
      (if (and lrange rrange (== (.start rrange) (+ (.start lrange) (.n-elems lrange))))
        (Int64Range. (.start lrange) 1 (+ (.n-elems lrange) (.n-elems rrange)) nil)
        (let [^PrimitiveList llist (if (instance? PrimitiveList lhs)
                                     lhs
                                     (doto (dtype-list/make-list index-space)
                                       (.addAll lrange)))]
          (.addAll llist rhs)
          llist)))))


(defn bool-reader->indexes
  "Given a reader, produce a filtered list of indexes filtering out 'false' values."
  (^PrimitiveList [{:keys [storage-type]} bool-item]
   (let [n-elems (dtype-base/ecount bool-item)
         reader (dtype-base/->reader bool-item)
         storage-type (or storage-type
                          (reader-index-space bool-item))]
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long len]
        (let [start-idx (long start-idx)
              len (long len)
              ^PrimitiveList idx-data (case storage-type
                                        :int32 (make-index-list :int32)
                                        ;; :bitmap `(RoaringBitmap.)
                                        :int64 (make-index-list :int64))]
          (dotimes [iter len]
            (let [iter-idx (unchecked-add start-idx iter)]
              (when (.readBoolean reader iter-idx)
                (.addLong idx-data iter-idx))))
          @idx-data))
      (partial reduce #(merge-index-list-results storage-type %1 %2)))))
  (^PrimitiveList [bool-item]
   (bool-reader->indexes nil bool-item)))
