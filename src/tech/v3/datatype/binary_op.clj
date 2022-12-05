(ns tech.v3.datatype.binary-op
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.reductions :as reductions]
            [ham-fisted.api :as hamf]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype LongReader DoubleReader ObjectReader
            BinaryOperator Buffer BinaryOperators$DoubleBinaryOperator
            BinaryOperators$LongBinaryOperator
            UnaryOperator UnaryOperators$LongUnaryOperator
            UnaryOperators$DoubleUnaryOperator]
           [clojure.lang IFn IFn$LLL IFn$DDD]
           [ham_fisted Casts]))


(set! *warn-on-reflection* true)


(defn ->operator
  (^BinaryOperator [item _opname]
   (cond
     (instance? BinaryOperator item)
     item
     (instance? java.util.function.BinaryOperator item)
     (let [^java.util.function.BinaryOperator item item]
       (reify BinaryOperator
         (binaryObject [this lhs rhs]
           (.apply item lhs rhs))))
     (instance? IFn$LLL item)
     (with-meta
       (reify BinaryOperators$LongBinaryOperator
         (binaryLong [this l r]
           (.invokePrim ^IFn$LLL item l r)))
       {:operation-space :int64})
     (instance? IFn$DDD item)
     (with-meta
       (reify BinaryOperators$DoubleBinaryOperator
         (binaryDouble [this l r]
           (.invokePrim ^IFn$DDD item l r)))
       {:operation-space :float64})
     :else
     (with-meta (reify BinaryOperator
                  (binaryObject [this lhs rhs]
                    (item lhs rhs)))
       {:operation-space :object})))
  (^BinaryOperator [item] (->operator item :_unnamed)))


(defn iterable
  (^Iterable [binary-op res-dtype lhs rhs]
   (let [binary-op (->operator binary-op)
         lhs (dtype-base/->iterable lhs)]
     (cond
       (casting/integer-type? res-dtype)
       (dispatch/typed-map-2 (fn [^long lhs ^long rhs]
                               (.binaryLong binary-op lhs rhs))
                             :int64 lhs rhs)
       (casting/float-type? res-dtype)
       (dispatch/typed-map-2 (fn [^double lhs ^double rhs]
                               (.binaryDouble binary-op lhs rhs))
                             :float64 lhs rhs)
       :else
       (dispatch/typed-map-2 binary-op res-dtype lhs rhs))))
  (^Iterable [binary-op lhs rhs]
   (iterable binary-op (dtype-base/elemwise-datatype lhs) lhs rhs)))


(defn- unary-op-space
  [op scalar vec-dtype]
  (let [op-space (casting/widest-datatype (dtype-proto/datatype scalar)
                                          vec-dtype)]
    (-> (if-let [op-op-space (get (meta op) :operation-space)]
          (casting/widest-datatype op-space op-op-space)
          op-space)
        (casting/simple-operation-space))))


(defn unary-op-l
  "Create a unary op from a binary op and a constant passed in as the left argument"
  [^BinaryOperator b v dtype]
  (let [op-space (unary-op-space b v dtype)]
    (-> (case op-space
          :int64
          (let [v (Casts/longCast v)]
            (reify UnaryOperators$LongUnaryOperator
              (unaryLong [this vv]
                (.binaryLong b v vv))))
          :float64
          (let [v (Casts/doubleCast v)]
            (reify UnaryOperators$DoubleUnaryOperator
              (unaryDouble [this vv]
                (.binaryDouble b v vv))))
          (reify UnaryOperator
            (unaryObject [this vv]
              (.binaryObject b v vv))))
        (with-meta {:operation-space op-space}))))

(defn unary-op-r
  "Create a unary op from a binary op and a constant passed in as the right argument"
  [^BinaryOperator b v dtype]
  (let [op-space (unary-op-space b v dtype)]
    (-> (case op-space
          :int64
          (let [v (Casts/longCast v)]
            (reify UnaryOperators$LongUnaryOperator
              (unaryLong [this vv]
                (.binaryLong b vv v))))
          :float64
          (let [v (Casts/doubleCast v)]
            (reify UnaryOperators$DoubleUnaryOperator
              (unaryDouble [this vv]
                (.binaryDouble b vv v))))
          (reify UnaryOperator
            (unaryObject [this vv]
              (.binaryObject b vv v))))
        (with-meta {:operation-space op-space}))))


(defn reader
  (^Buffer [^BinaryOperator binop res-dtype lhs rhs]
   (let [op-space (or (get (meta binop) :operation-space) res-dtype)
         lhs (dtype-base/->reader lhs op-space)
         rhs (dtype-base/->reader rhs op-space)
         n-elems (.lsize lhs)
         binop (->operator binop)]
     (case op-space
       :int64
       (reify LongReader
         (elemwiseDatatype [rdr] res-dtype)
         (lsize [rdr] n-elems)
         (readLong [rdr idx] (.binaryLong binop
                                          (.readLong lhs idx)
                                          (.readLong rhs idx))))
       :float64
       (reify DoubleReader
         (elemwiseDatatype [rdr] res-dtype)
         (lsize [rdr] n-elems)
         (readDouble [rdr idx] (.binaryDouble binop
                                              (.readDouble lhs idx)
                                              (.readDouble rhs idx))))
       (case (casting/simple-operation-space res-dtype)
         :int64
         (reify LongReader
           (elemwiseDatatype [rdr] res-dtype)
           (lsize [rdr] n-elems)
           (readLong [rdr idx]
             (Casts/longCast (.binaryObject binop
                                            (.readObject lhs idx)
                                            (.readObject rhs idx)))))
         :float64
         (reify DoubleReader
           (elemwiseDatatype [rdr] res-dtype)
           (lsize [rdr] n-elems)
           (readDouble [rdr idx]
             (Casts/doubleCast (.binaryObject binop
                                              (.readObject lhs idx)
                                              (.readObject rhs idx)))))
         (reify ObjectReader
           (elemwiseDatatype [rdr] res-dtype)
           (lsize [rdr] n-elems)
           (readObject [rdr idx]
             (.binaryObject binop
                            (.readObject lhs idx)
                            (.readObject rhs idx))))))))
  (^Buffer [binop lhs rhs]
   (reader binop (casting/widest-datatype
                  (dtype-base/elemwise-datatype lhs)
                  (dtype-base/elemwise-datatype rhs))
           lhs rhs)))

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



(def builtin-ops
  (->> [(make-numeric-object-binary-op :tech.numerics/+ (pmath/+ x y) (+ x y) 0)
        (make-numeric-object-binary-op :tech.numerics/- (pmath/- x y) (- x y) 0)
        (make-numeric-object-binary-op :tech.numerics// (pmath// x y) (/ x y) 1)
        (make-numeric-object-binary-op :tech.numerics/* (pmath/* x y) (* x y) 1)
        (make-numeric-object-binary-op :tech.numerics/rem (rem x y) (rem x y) 1)
        (make-numeric-object-binary-op :tech.numerics/quot (quot x y) (quot x y) 1)
        (make-float-double-binary-op :tech.numerics/pow (Math/pow x y) 1)
        (make-numeric-object-binary-op :tech.numerics/max (if (pmath/> x y) x y) (if (> x y) x y)
                                       (- Double/MAX_VALUE))
        (make-numeric-object-binary-op :tech.numerics/min (if (pmath/> x y) y x) (if (> x y) y x)
                                       Double/MAX_VALUE)
        (make-int-long-binary-op :tech.numerics/bit-and (pmath/bit-and x y))
        (make-int-long-binary-op :tech.numerics/bit-and-not (bit-and-not x y))
        (make-int-long-binary-op :tech.numerics/bit-or (pmath/bit-or x y))
        (make-int-long-binary-op :tech.numerics/bit-xor (pmath/bit-xor x y))
        (make-int-long-binary-op :tech.numerics/bit-clear (bit-clear x y))
        (make-int-long-binary-op :tech.numerics/bit-flip (bit-flip x y))
        (make-int-long-binary-op :tech.numerics/bit-test (bit-test x y))
        (make-int-long-binary-op :tech.numerics/bit-set (bit-set x y))
        (make-int-long-binary-op :tech.numerics/bit-shift-left (pmath/bit-shift-left x y))
        (make-int-long-binary-op :tech.numerics/bit-shift-right (pmath/bit-shift-right x y))
        (make-int-long-binary-op :tech.numerics/unsigned-bit-shift-right
                                 (unsigned-bit-shift-right x y))
        (make-float-double-binary-op :tech.numerics/atan2 (Math/atan2 x y))
        (make-float-double-binary-op :tech.numerics/hypot (Math/hypot x y))
        (make-float-double-binary-op :tech.numerics/ieee-remainder (Math/IEEEremainder x y))]
       (map #(vector (dtype-proto/op-name %) %))
       (into {})))


(defn commutative-binary-reduce
  "Perform a commutative binary reduction.  The space of the reduction will
  be determined by the datatype of the reader."
  [op rdr]
  (reductions/commutative-binary-reduce op rdr))
