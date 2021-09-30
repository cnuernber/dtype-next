(ns tech.v3.datatype.unary-op
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :refer [get-significand]]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype LongReader DoubleReader ObjectReader
            UnaryOperator Buffer
            UnaryOperators$DoubleUnaryOperator
            UnaryOperators$ObjectUnaryOperator]
           [clojure.lang IFn]
           [java.util.function DoubleUnaryOperator]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ->operator
  (^UnaryOperator [item opname]
   (cond
     (instance? UnaryOperator item)
     item
     (instance? DoubleUnaryOperator item)
     (let [^DoubleUnaryOperator item item]
       (reify UnaryOperators$DoubleUnaryOperator
         (unaryDouble [this arg]
           (.applyAsDouble item arg))))
     (instance? java.util.function.UnaryOperator item)
     (let [^java.util.function.UnaryOperator item item]
       (reify UnaryOperators$ObjectUnaryOperator
         (unaryObject [this arg]
           (.apply item arg))))
     (instance? IFn item)
     (let [^IFn item item]
       (reify UnaryOperators$ObjectUnaryOperator
         (unaryObject [this arg]
           (.invoke item arg))))))
  (^UnaryOperator [item] (->operator item :_unnamed)))


(defn iterable
  (^Iterable [unary-op res-dtype lhs]
   (let [unary-op (->operator unary-op)
         lhs (dtype-base/->iterable lhs)]
     (cond
       (casting/integer-type? res-dtype)
       (dispatch/typed-map-1 (fn [^long arg]
                               (.unaryLong unary-op arg))
                             :int64 lhs)
       (casting/float-type? res-dtype)
       (dispatch/typed-map-1 (fn [^double arg]
                               (.unaryDouble unary-op arg))
                             :float64 lhs)
       :else
       (dispatch/typed-map-1 unary-op res-dtype lhs))))
  (^Iterable [unary-op lhs]
   (iterable unary-op (dtype-base/elemwise-datatype lhs) lhs)))


(defn reader
  (^Buffer [unary-op res-dtype lhs]
   (let [unary-op (->operator unary-op)
         op-space (casting/simple-operation-space res-dtype)
         lhs (dtype-base/->reader lhs op-space)
         n-elems (.lsize lhs)]
     (case op-space
       :int64
       (reify LongReader
         (elemwiseDatatype [rdr] res-dtype)
         (lsize [rdr] n-elems)
         (readLong [rdr idx] (.unaryLong unary-op (.readLong lhs idx))))
       :float64
       (reify DoubleReader
         (elemwiseDatatype [rdr] res-dtype)
         (lsize [rdr] n-elems)
         (readDouble [rdr idx] (.unaryDouble unary-op (.readDouble lhs idx))))
       (reify ObjectReader
         (elemwiseDatatype [rdr] res-dtype)
         (lsize [rdr] n-elems)
         (readObject [rdr idx] (.unaryObject unary-op (.readObject lhs idx)))))))
  (^Buffer [^UnaryOperator unary-op lhs]
   (let [lhs-dtype (dtype-base/elemwise-datatype lhs)
         op-dtype (if (instance? IFn unary-op)
                    :object
                    lhs-dtype)]
     (reader unary-op op-dtype lhs))))


(defmacro make-double-unary-op
  [opname opcode]
  `(-> (reify
         dtype-proto/POperator
         (op-name [item#] ~opname)
         UnaryOperator
         (unaryBoolean [this# x#]
           (throw (Exception. (format "op %s not defined for boolean" ~opname))))
         (unaryByte [this# x#]
           (throw (Exception. (format "op %s not defined for byte" ~opname))))
         (unaryShort [this# x#]
           (throw (Exception. (format "op %s not defined for short" ~opname))))
         (unaryChar [this# x#]
           (throw (Exception. (format "op %s not defined for char" ~opname))))
         (unaryInt [this# x#]
           (throw (Exception. (format "op %s not defined for int" ~opname))))
         (unaryLong [this# x#]
           (throw (Exception. (format "op %s not defined for long" ~opname))))
         (unaryFloat [this# x#]
           (throw (Exception. (format "op %s not defined for float" ~opname))))
         (unaryDouble [this# ~'x] ~opcode)
         (unaryObject [this# x#] (.unaryDouble this# x#)))
       (vary-meta assoc :operation-space :float64)))


(defmacro make-numeric-object-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     UnaryOperator
     (unaryBoolean [this# x#]
       (throw (Exception. (format "op %s not defined for boolean" ~opname))))
     (unaryByte [this# ~'x] (byte ~opcode))
     (unaryShort [this# ~'x] (short ~opcode))
     (unaryChar [this# ~'x]
       (throw (Exception. (format "op %s not defined for char" ~opname))))
     (unaryInt [this# ~'x] ~opcode)
     (unaryLong [this# ~'x] ~opcode)
     (unaryFloat [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# ~'x]
       (if (casting/integer-type? (dtype-base/elemwise-datatype ~'x))
         (let [~'x (unchecked-long ~'x)]
           ~opcode)
         (let [~'x (unchecked-double ~'x)]
           ~opcode)))))


(defmacro make-float-double-unary-op
  [opname opcode]
  `(-> (reify
         dtype-proto/POperator
         (op-name [item#] ~opname)
         UnaryOperators$DoubleUnaryOperator
         (unaryFloat [this# ~'x] ~opcode)
         (unaryDouble [this# ~'x] ~opcode)
         (unaryObject [this# x#] (.unaryDouble this# x#)))
       (vary-meta assoc :operation-space :float32)))


(defmacro make-numeric-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     UnaryOperator
     (unaryByte [this# ~'x] (byte ~opcode))
     (unaryShort [this# ~'x] (short ~opcode))
     (unaryChar [this# ~'x]
       (throw (Exception. (format "op %s not defined for char" ~opname))))
     (unaryInt [this# ~'x] ~opcode)
     (unaryLong [this# ~'x] ~opcode)
     (unaryFloat [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# x#] (.unaryDouble this# x#))))


(defmacro make-long-unary-op
  [opname opcode]
  `(-> (reify
         dtype-proto/POperator
         (op-name [item#] ~opname)
         UnaryOperator
         (unaryBoolean [this# ~'x]
           (throw (Exception. (format "op %s not defined for boolean" ~opname))))
         (unaryByte [this# ~'x]
           (throw (Exception. (format "op %s not defined for byte" ~opname))))
         (unaryShort [this# ~'x]
           (throw (Exception. (format "op %s not defined for short" ~opname))))
         (unaryChar [this# x#]
           (throw (Exception. (format "op %s not defined for char" ~opname))))
         (unaryInt [this# ~'x]
           (throw (Exception. (format "op %s not defined for char" ~opname))))
         (unaryLong [this# ~'x] ~opcode)
         (unaryFloat [this# ~'x]
           (throw (Exception. (format "op %s not defined for float" ~opname))))
         (unaryDouble [this# ~'x]
           (throw (Exception. (format "op %s not defined for double" ~opname))))
         (unaryObject [this# x#] (.unaryLong this# (long x#))))
       (vary-meta assoc :operation-space :int64)))


(defmacro make-all-datatype-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     UnaryOperator
     (unaryBoolean [this# ~'x] ~opcode)
     (unaryByte [this# ~'x] ~opcode)
     (unaryShort [this# ~'x] ~opcode)
     (unaryChar [this# ~'x] ~opcode)
     (unaryInt [this# ~'x] ~opcode)
     (unaryLong [this# ~'x] ~opcode)
     (unaryFloat [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# ~'x] ~opcode)))


(def builtin-ops
  (->> [(make-double-unary-op :tech.numerics/floor (Math/floor x))
        (make-double-unary-op :tech.numerics/ceil (Math/ceil x))
        (make-double-unary-op :tech.numerics/round (unchecked-double (Math/round (double x))))
        (make-double-unary-op :tech.numerics/rint (Math/rint x))
        (make-double-unary-op :tech.numerics/get-significand (unchecked-double (get-significand x)))
        (make-numeric-object-unary-op :tech.numerics/- (- x))
        (make-float-double-unary-op :tech.numerics/logistic
                                    (/ 1.0
                                       (+ 1.0 (Math/exp (- x)))))
        (make-float-double-unary-op :tech.numerics/exp (Math/exp x))
        (make-float-double-unary-op :tech.numerics/expm1 (Math/expm1 x))
        (make-float-double-unary-op :tech.numerics/log (Math/log x))
        (make-float-double-unary-op :tech.numerics/log10 (Math/log10 x))
        (make-float-double-unary-op :tech.numerics/log1p (Math/log1p x))
        (make-float-double-unary-op :tech.numerics/signum (Math/signum x))
        (make-float-double-unary-op :tech.numerics/sqrt (Math/sqrt x))
        (make-float-double-unary-op :tech.numerics/cbrt (Math/cbrt x))
        (make-float-double-unary-op :tech.numerics/abs (Math/abs x))
        (make-numeric-unary-op :tech.numerics/sq (unchecked-multiply x x))
        (make-float-double-unary-op :tech.numerics/sin (Math/sin x))
        (make-float-double-unary-op :tech.numerics/sinh (Math/sinh x))
        (make-float-double-unary-op :tech.numerics/cos (Math/cos x))
        (make-float-double-unary-op :tech.numerics/cosh (Math/cosh x))
        (make-float-double-unary-op :tech.numerics/tan (Math/tan x))
        (make-float-double-unary-op :tech.numerics/tanh (Math/tanh x))
        (make-float-double-unary-op :tech.numerics/acos (Math/acos x))
        (make-float-double-unary-op :tech.numerics/asin (Math/asin x))
        (make-float-double-unary-op :tech.numerics/atan (Math/atan x))
        (make-float-double-unary-op :tech.numerics/to-degrees (Math/toDegrees x))
        (make-float-double-unary-op :tech.numerics/to-radians (Math/toRadians x))
        (make-float-double-unary-op :tech.numerics/next-up (Math/nextUp x))
        (make-float-double-unary-op :tech.numerics/next-down (Math/nextDown x))
        (make-float-double-unary-op :tech.numerics/ulp (Math/ulp x))
        (make-long-unary-op :tech.numerics/bit-not (bit-not x))
        (make-numeric-unary-op :tech.numerics// (/ x))
        (make-all-datatype-unary-op :tech.numerics/identity x)
        (make-all-datatype-unary-op :tech.numerics/+ x)]
       (map #(vector (dtype-proto/op-name %) %))
       (into {})))
