(ns tech.v3.datatype.unary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :as double-ops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.parallel.for :as parallel-for])
  (:import [tech.v3.datatype UnaryPredicate
            UnaryPredicates$BooleanUnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$LongUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate
            BooleanReader LongReader DoubleReader ObjectReader
            PrimitiveList]
           [java.util List]
           [java.util.function DoublePredicate Predicate]
           [clojure.lang IFn]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


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
         (op-name [this] opname)))))
  (^UnaryPredicate [item]
   (->predicate item :_unnamed)))


(defn iterable
  [pred src]
  (let [pred (->predicate pred)
        src (dtype-base/ensure-iterable src)]
    (dispatch/typed-map-1 pred :boolean src)))


(defn reader
  [pred src-rdr]
  (let [pred (->predicate pred)
        src-rdr (dtype-base/->reader src-rdr)
        src-dtype (dtype-base/elemwise-datatype src-rdr)]
    (cond
      (= :boolean src-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryBoolean pred (.readBoolean src-rdr idx))))
      (casting/integer-type? src-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryLong pred (.readLong src-rdr idx))))
      (casting/float-type? src-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryDouble pred (.readDouble src-rdr idx))))
      :else
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
  {:not
   (reify
     UnaryPredicates$BooleanUnaryPredicate
     (unaryBoolean [this arg]
       (if arg false true))
     dtype-proto/POperator
     (op-name [this] :not))
   :nan?
   (reify
     UnaryPredicates$DoubleUnaryPredicate
     (unaryDouble [this arg]
       (Double/isNaN arg))
     dtype-proto/POperator
     (op-name [this] :nan?))
   :finite?
   (reify
     UnaryPredicates$DoubleUnaryPredicate
     (unaryDouble [this arg]
       (Double/isFinite arg))
     dtype-proto/POperator
     (op-name [this] :finite?))
   :infinite?
   (reify
     UnaryPredicates$DoubleUnaryPredicate
     (unaryDouble [this arg]
       (Double/isInfinite arg))
     dtype-proto/POperator
     (op-name [this] :infinite?))
   :mathematical-integer?
   (reify
     UnaryPredicates$DoubleUnaryPredicate
     (unaryDouble [this arg]
       (double-ops/is-mathematical-integer? arg))
     dtype-proto/POperator
     (op-name [this] :mathematical-integer?))
   :pos?
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
   :neg?
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
   :even? (ifn->long-unary-predicate even? :even?)
   :odd? (ifn->long-unary-predicate even? :odd?)
   :zero?
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


(defn bool-reader->indexes
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
                                        :int32 (dtype-list/make-list :int32)
                                        ;; :bitmap `(RoaringBitmap.)
                                        :int64 (dtype-list/make-list :int64))]
          (dotimes [iter len]
            (let [iter-idx (unchecked-add start-idx iter)]
              (when (.readBoolean reader iter-idx)
                (.addLong idx-data (unchecked-add start-idx iter)))))
          idx-data))
      (partial reduce (fn [^List lhs ^List rhs]
                        (.addAll lhs rhs)
                        lhs)))))
  (^PrimitiveList [bool-item]
   (bool-reader->indexes bool-item)))
