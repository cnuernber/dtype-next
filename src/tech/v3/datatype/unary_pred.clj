(ns tech.v3.datatype.unary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :as double-ops])
  (:import [tech.v3.datatype UnaryPredicate
            UnaryPredicates$BooleanUnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate]))



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
       (pos? arg))
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
       (neg? arg))
     dtype-proto/POperator
     (op-name [this] :neg?))
   :zero?
   (reify
     UnaryPredicates$ObjectUnaryPredicate
     (unaryLong [this arg]
       (zero? arg))
     (unaryDouble [this arg]
       (zero? arg))
     (unaryObject [this arg]
       (zero? arg))
     dtype-proto/POperator
     (op-name [this] :zero?))})
