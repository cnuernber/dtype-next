(ns tech.v3.datatype.unary-op
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :as double-ops]
            [ham-fisted.api :as hamf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.defprotocol :refer [extend extend-type extend-protocol]]
            [ham-fisted.protocols :as hamf-proto])
  (:import [tech.v3.datatype LongReader DoubleReader ObjectReader
            UnaryOperator Buffer
            UnaryOperators$DoubleUnaryOperator UnaryOperators$ObjDoubleUnaryOperator
            UnaryOperators$LongUnaryOperator UnaryOperators$ObjLongUnaryOperator]
           [clojure.lang IFn IFn$LL IFn$DD IFn$OLO IFn$ODO IFn$OL IFn$OD IFn$DL IFn$LD]
           [java.util.function DoubleUnaryOperator LongUnaryOperator]
           [ham_fisted Casts Reductions IFnDef$OLO IFnDef$ODO])
  (:refer-clojure :exclude [extend extend-type extend-protocol
                            - min max abs bit-not / + identity
                            double long]))


(set! *warn-on-reflection* true)

(defmacro define-input-output-datatypes
  [cls in-type out-type]
  `(extend-type ~cls
     dtype-proto/PInputDatatype (input-datatype [this#] ~in-type)
     hamf-proto/ReturnedDatatype (returned-datatype [this#] ~out-type)))


(define-input-output-datatypes UnaryOperators$DoubleUnaryOperator :float64 :float64)
(define-input-output-datatypes UnaryOperators$LongUnaryOperator :int64 :int64)

(defmacro make-double-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator (op-name [this#] ~opname)
     UnaryOperators$DoubleUnaryOperator
     (unaryDouble [this# ~'x] ~opcode)))

(defmacro make-numeric-object-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator (op-name [this#] ~opname)
     UnaryOperator
     (unaryLong [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# ~'x]
       (if (casting/integer-type? (dtype-base/datatype ~'x))
         (let [~'x (Casts/longCast ~'x)]
           ~opcode)
         (let [~'x (Casts/doubleCast ~'x)]
           ~opcode)))))

(defmacro make-float-double-unary-op
  ([opname opcode]
   `(reify
      dtype-proto/POperator (op-name [this#] ~opname)
      UnaryOperators$DoubleUnaryOperator
      (unaryDouble [this# ~'x] ~opcode))))

(defmacro make-numeric-unary-op
  ([opname opcode]
   `(make-numeric-unary-op ~opname ~opcode ~opcode))
  ([opname prim-opcode obj-opcode]
   `(reify
      dtype-proto/POperator (op-name [this#] ~opname)
      UnaryOperator
      (unaryLong [this# ~'x] ~prim-opcode)
      (unaryDouble [this# ~'x] ~prim-opcode)
      (unaryObject [this# ~'x] ~obj-opcode)
      dtype-proto/PInputDatatype (input-datatype [this] nil)
      hamf-proto/ReturnedDatatype (returned-datatype [this] nil))))

(defmacro make-long-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator (op-name [this#] ~opname)
     UnaryOperators$LongUnaryOperator
     (unaryLong [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x]
       (throw (Exception. "op not defined for double")))
     (unaryObject [this# x#] (.unaryLong this# (Casts/longCast x#)))))

(defmacro make-all-datatype-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator (op-name [this#] ~opname)
     UnaryOperator
     (unaryLong [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# ~'x] ~opcode)
     dtype-proto/PInputDatatype (input-datatype [this] nil)
     hamf-proto/ReturnedDatatype (returned-datatype [this] nil)))

(def double (reify
              dtype-proto/POperator (op-name [this] :tech.numerics/double)
              UnaryOperators$ObjDoubleUnaryOperator
              (unaryObjDouble [this x] (if (string? x)
                                         (Double/parseDouble x)
                                         (Casts/doubleCast x)))
              dtype-proto/PInputDatatype (input-datatype [this] :object)
              hamf-proto/ReturnedDatatype (returned-datatype [this] :float64)))
(def long (reify
            dtype-proto/POperator (op-name [this] :tech.numerics/long)
            UnaryOperators$ObjLongUnaryOperator
            (unaryObjLong [this x] (if (string? x)
                                     (Long/parseLong x)
                                     (Casts/longCast x)))
            dtype-proto/PInputDatatype (input-datatype [this] :object)
            hamf-proto/ReturnedDatatype (returned-datatype [this] :int64)))
(def floor (make-double-unary-op :tech.numerics/floor (Math/floor x)))
(def ceil (make-double-unary-op :tech.numerics/ceil (Math/ceil x)))
(def rint (make-double-unary-op :tech.numerics/rint (Math/rint x)))
(def get-significand (make-double-unary-op :tech.numerics/get-significand (unchecked-double (double-ops/get-significand x))))
(def - (make-numeric-object-unary-op :tech.numerics/- (clojure.core/- x)))
(def min (make-numeric-object-unary-op :tech.numerics/min (clojure.core/min x)))
(def max (make-numeric-object-unary-op :tech.numerics/max (clojure.core/max x)))
(def logistic (make-float-double-unary-op :tech.numerics/logistic
                                          (clojure.core// 1.0
                                                          (clojure.core/+ 1.0 (Math/exp (clojure.core/- x))))))
(def round (reify
             dtype-proto/POperator (op-name [this] :tech.numerics/round)
             UnaryOperator
             (unaryObject [this v] (.unaryObjLong this v))
             (unaryObjLong [this v] (Math/round (Casts/doubleCast v)))
             dtype-proto/PInputDatatype (input-datatype [this] :object)
             hamf-proto/ReturnedDatatype (returned-datatype [this] :int64)))

(def exp (make-float-double-unary-op :tech.numerics/exp (Math/exp x)))
(def expm1 (make-float-double-unary-op :tech.numerics/expm1 (Math/expm1 x)))
(def log (make-float-double-unary-op :tech.numerics/log (Math/log x)))
(def log10 (make-float-double-unary-op :tech.numerics/log10 (Math/log10 x)))
(def log1p (make-float-double-unary-op :tech.numerics/log1p (Math/log1p x)))
(def signum (make-float-double-unary-op :tech.numerics/signum (Math/signum x)))
(def sqrt (make-float-double-unary-op :tech.numerics/sqrt (Math/sqrt x)))
(def cbrt (make-float-double-unary-op :tech.numerics/cbrt (Math/cbrt x)))
(def abs (make-float-double-unary-op :tech.numerics/abs (Math/abs x)))
(def sq (make-numeric-unary-op :tech.numerics/sq (* x x) (clojure.lang.Numbers/multiply x x)))
(def sin (make-float-double-unary-op :tech.numerics/sin (Math/sin x)))
(def sinh (make-float-double-unary-op :tech.numerics/sinh (Math/sinh x)))
(def cos (make-float-double-unary-op :tech.numerics/cos (Math/cos x)))
(def cosh (make-float-double-unary-op :tech.numerics/cosh (Math/cosh x)))
(def tan (make-float-double-unary-op :tech.numerics/tan (Math/tan x)))
(def tanh (make-float-double-unary-op :tech.numerics/tanh (Math/tanh x)))
(def acos (make-float-double-unary-op :tech.numerics/acos (Math/acos x)))
(def asin (make-float-double-unary-op :tech.numerics/asin (Math/asin x)))
(def atan (make-float-double-unary-op :tech.numerics/atan (Math/atan x)))
(def to-degrees (make-float-double-unary-op :tech.numerics/to-degrees (Math/toDegrees x)))
(def to-radians (make-float-double-unary-op :tech.numerics/to-radians (Math/toRadians x)))
(def next-up (make-float-double-unary-op :tech.numerics/next-up (Math/nextUp x)))
(def next-down (make-float-double-unary-op :tech.numerics/next-down (Math/nextDown x)))
(def ulp (make-float-double-unary-op :tech.numerics/ulp (Math/ulp x)))
(def bit-not (make-long-unary-op :tech.numerics/bit-not (clojure.core/bit-not x)))
(def / (make-numeric-unary-op :tech.numerics// (clojure.core// x)))
(def identity (make-all-datatype-unary-op :tech.numerics/identity x))
(def + (make-all-datatype-unary-op :tech.numerics/+ x))

(def builtin-ops
  (->> [double long floor ceil rint get-significand - min max logistic exp expm1 log log10 log1p
        signum sqrt cbrt abs sq sin sinh cos cosh tan tanh acos asin atan round
        to-degrees to-radians next-up next-down ulp bit-not / identity +]
       (map #(vector (dtype-proto/op-name %) %))
       (into {})))

(defn ->unary-operator
  ^UnaryOperator [item]
  (cond
    (instance? UnaryOperator item)
    item
    (instance? clojure.lang.Keyword item)
    (if-let [rv (get builtin-ops item)]
      rv
      (throw (RuntimeException. (str "Failed to find unary operator for " item))))
    (instance? DoubleUnaryOperator item)
    (reify UnaryOperators$DoubleUnaryOperator
      (unaryDouble [this arg]
        (.applyAsDouble ^DoubleUnaryOperator item arg)))
    (instance? LongUnaryOperator item)
    (reify UnaryOperators$LongUnaryOperator
      (unaryLong [this arg]
        (.applyAsLong ^LongUnaryOperator item arg)))
    (instance? java.util.function.UnaryOperator item)
    (reify UnaryOperator
      (unaryObject [this arg]
        (.apply ^java.util.function.UnaryOperator item arg)))
    (instance? IFn$LL item)
    (reify UnaryOperators$LongUnaryOperator
      (unaryLong [_t v]
        (.invokePrim ^IFn$LL item v)))
    (instance? IFn$DD item)
    (reify UnaryOperators$DoubleUnaryOperator
      (unaryDouble [_t v]
        (.invokePrim ^IFn$DD item v)))
    (instance? IFn$OL item)
    (reify UnaryOperator
      (unaryObject [_t v] (.invokePrim ^IFn$OL item v))
      (unaryObjLong [_t v] (.invokePrim ^IFn$OL item v))
      dtype-proto/PInputDatatype (input-datatype [this] :object)
      hamf-proto/ReturnedDatatype (returned-datatype [this] :int64))
    (instance? IFn$OD item)
    (reify UnaryOperator
      (unaryObject [_t v] (.invokePrim ^IFn$OD item v))
      (unaryObjDouble [_t v] (.invokePrim ^IFn$OD item v))
      dtype-proto/PInputDatatype (input-datatype [this] :object)
      hamf-proto/ReturnedDatatype (returned-datatype [this] :float64))
    :else
    (reify UnaryOperator
      (unaryObject [this arg]
        (item arg))
      dtype-proto/PInputDatatype (input-datatype [this] :object)
      hamf-proto/ReturnedDatatype (returned-datatype [this] :object))))
