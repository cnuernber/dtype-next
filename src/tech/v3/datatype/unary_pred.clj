(ns tech.v3.datatype.unary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :as double-ops]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base])
  (:import [tech.v3.datatype UnaryPredicate
            UnaryPredicates$BooleanUnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$LongUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate
            BooleanReader LongReader DoubleReader ObjectReader]
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

(defn reader
  [src-rdr ^UnaryPredicate pred]
  (let [src-rdr (dtype-base/->reader src-rdr)
        src-dtype (dtype-base/elemwise-datatype src-rdr)]
    (cond
      (= :boolean src-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryBoolean pred (.readBoolean rdr idx))))
      (casting/integer-type? src-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryLong pred (.readLong rdr idx))))
      (casting/float-type? src-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryDouble pred (.readDouble rdr idx))))
      :else
      (reify BooleanReader
        (lsize [rdr] (.lsize src-rdr))
        (readBoolean [rdr idx]
          (.unaryObject pred (.readObject rdr idx)))))))


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
