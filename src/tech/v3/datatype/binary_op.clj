(ns tech.v3.datatype.binary-op
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.reductions :as reductions]
            [tech.v3.datatype.unary-op :as unary-op]
            [ham-fisted.api :as hamf]
            [clj-commons.primitive-math :as pmath])
  (:import [tech.v3.datatype LongReader DoubleReader ObjectReader
            BinaryOperator Buffer BinaryOperators$DoubleBinaryOperator
            BinaryOperators$LongBinaryOperator
            UnaryOperator UnaryOperators$LongUnaryOperator
            UnaryOperators$DoubleUnaryOperator]
           [clojure.lang IFn IFn$LLL IFn$DDD]
           [ham_fisted Casts])
  (:refer-clojure :exclude [+ - / * rem quot max min bit-and bit-and-not bit-or bit-xor bit-clear bit-flip
                            bit-set bit-shift-left bit-shift-right unsigned-bit-shift-right]))


(set! *warn-on-reflection* true)

(unary-op/define-input-output-datatypes BinaryOperators$LongBinaryOperator :int64 :int64)
(unary-op/define-input-output-datatypes BinaryOperators$DoubleBinaryOperator :float64 :float64)

(defn- as-number
  ^Number [x] (casting/->number x))

(defmacro make-numeric-object-binary-op
  ([opname scalar-op object-op identity-value]
   `(reify
      dtype-proto/POperator
      (op-name [item#] ~opname)
      BinaryOperator
      (binaryLong [this# ~'x ~'y] ~scalar-op)
      (binaryDouble [this# ~'x ~'y] ~scalar-op)
      (binaryObject [this# ~'x ~'y]
        (let [~'x (as-number ~'x)
              ~'y (as-number ~'y)]
          ~object-op))
      (initialDoubleReductionValue [this] (double ~identity-value))))
  ([opname scalar-op object-op]
   `(make-numeric-object-binary-op ~opname ~scalar-op ~object-op 1)))


(defmacro make-float-double-binary-op
  ([opname scalar-op identity-value]
   `(-> (reify
          dtype-proto/POperator
          (op-name [item#] ~opname)
          BinaryOperators$DoubleBinaryOperator
          (binaryDouble [this# ~'x ~'y] ~scalar-op)
          (initialDoubleReductionValue [this] (double ~identity-value)))
        (vary-meta assoc :operation-space :float32)))
  ([opname scalar-op]
   `(make-float-double-binary-op ~opname ~scalar-op 1.0)))


(defmacro make-int-long-binary-op
  [opname scalar-op]
  `(-> (reify
         dtype-proto/POperator
         (op-name [item#] ~opname)
         BinaryOperators$LongBinaryOperator
         (binaryLong [this# ~'x ~'y] ~scalar-op))
       (vary-meta assoc :operation-space :int32)))

(def + (make-numeric-object-binary-op :tech.numerics/+ (pmath/+ x y) (+ x y) 0))
(def - (make-numeric-object-binary-op :tech.numerics/- (pmath/- x y) (- x y) 0))
(def / (make-numeric-object-binary-op :tech.numerics// (pmath// x y) (/ x y) 1))
(def * (make-numeric-object-binary-op :tech.numerics/* (pmath/* x y) (* x y) 1))
(def rem (make-numeric-object-binary-op :tech.numerics/rem (rem x y) (rem x y) 1))
(def quot (make-numeric-object-binary-op :tech.numerics/quot (quot x y) (quot x y) 1))
(def pow (make-float-double-binary-op :tech.numerics/pow (Math/pow x y) 1))
(def max (make-numeric-object-binary-op :tech.numerics/max (if (pmath/> x y) x y) (if (> x y) x y)
                                        (- Double/MAX_VALUE)))
(def min (make-numeric-object-binary-op :tech.numerics/min (if (pmath/> x y) y x) (if (> x y) y x)
                                        Double/MAX_VALUE))
(def bit-and (make-int-long-binary-op :tech.numerics/bit-and (pmath/bit-and x y)))
(def bit-and-not (make-int-long-binary-op :tech.numerics/bit-and-not (bit-and-not x y)))
(def bit-or (make-int-long-binary-op :tech.numerics/bit-or (pmath/bit-or x y)))
(def bit-xor (make-int-long-binary-op :tech.numerics/bit-xor (pmath/bit-xor x y)))
(def bit-clear (make-int-long-binary-op :tech.numerics/bit-clear (bit-clear x y)))
(def bit-flip (make-int-long-binary-op :tech.numerics/bit-flip (bit-flip x y)))
(def bit-set (make-int-long-binary-op :tech.numerics/bit-set (bit-set x y)))
(def bit-shift-left (make-int-long-binary-op :tech.numerics/bit-shift-left (pmath/bit-shift-left x y)))
(def bit-shift-right (make-int-long-binary-op :tech.numerics/bit-shift-right (pmath/bit-shift-right x y)))
(def unsigned-bit-shift-right (make-int-long-binary-op :tech.numerics/unsigned-bit-shift-right
                                                       (unsigned-bit-shift-right x y)))
(def atan2 (make-float-double-binary-op :tech.numerics/atan2 (Math/atan2 x y)))
(def hypot (make-float-double-binary-op :tech.numerics/hypot (Math/hypot x y)))
(def ieee-remainder (make-float-double-binary-op :tech.numerics/ieee-remainder (Math/IEEEremainder x y)))

(def builtin-ops
  (->> [+ - / * rem quot pow max min bit-and bit-and-not bit-or bit-xor
        bit-clear bit-flip bit-set bit-shift-left bit-shift-right
        unsigned-bit-shift-right atan2 hypot ieee-remainder]
       (map #(vector (dtype-proto/op-name %) %))
       (into {})))


(defn commutative-binary-reduce
  "Perform a commutative binary reduction.  The space of the reduction will
  be determined by the datatype of the reader."
  [op rdr]
  (reductions/commutative-binary-reduce op rdr))

(defn ->binary-operator
  ^BinaryOperator [item]
  (cond
    (instance? BinaryOperator item)
    item
    (instance? clojure.lang.Keyword item)
    (if-let [rv (get builtin-ops item)]
      rv
      (throw (RuntimeException. (str "Failed to find binary operator for " item))))
    (instance? java.util.function.BinaryOperator item)
    (let [^java.util.function.BinaryOperator item item]
      (reify BinaryOperator
        (binaryObject [this lhs rhs]
          (.apply item lhs rhs))))
    (instance? IFn$LLL item)
    (reify BinaryOperators$LongBinaryOperator
      (binaryLong [this l r]
        (.invokePrim ^IFn$LLL item l r)))
    (instance? IFn$DDD item)
    (reify BinaryOperators$DoubleBinaryOperator
      (binaryDouble [this l r]
        (.invokePrim ^IFn$DDD item l r)))
    :else
    (reify BinaryOperator
      (binaryObject [this lhs rhs]
        (item lhs rhs)))))
