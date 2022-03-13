(ns tech.v3.datatype.binary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.errors :as errors]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryPredicate Buffer
            BinaryPredicates$BooleanBinaryPredicate
            BinaryPredicates$LongBinaryPredicate
            BinaryPredicates$ObjectBinaryPredicate
            BooleanConversions
            BooleanReader ObjectReader
            UnaryPredicate
            UnaryPredicates$BooleanUnaryPredicate
            UnaryPredicates$LongUnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate]
           [clojure.lang IFn]
           [org.apache.commons.math3.util Precision]
           [java.util Comparator]))

(set! *warn-on-reflection* true)


(defn ifn->binary-predicate
  (^BinaryPredicate [ifn opname]
   (when-not (instance? IFn ifn)
     (errors/throwf "Arg (%s) is not an instance of IFn" ifn))
   (reify
     BinaryPredicates$ObjectBinaryPredicate
     (binaryObject [this lhs rhs]
       (BooleanConversions/from (ifn lhs rhs)))
     dtype-proto/POperator
     (op-name [this] opname)))
  (^BinaryPredicate [ifn]
   (ifn->binary-predicate ifn :_unnamed)))


(defn ifn->long-binary-predicate
  (^BinaryPredicate [ifn opname]
   (when-not (instance? IFn ifn)
     (errors/throwf "Arg (%s) is not an instance of IFn" ifn))
   (reify
     BinaryPredicates$LongBinaryPredicate
     (binaryObject [this lhs rhs]
       (BooleanConversions/from (ifn lhs rhs)))
     dtype-proto/POperator
     (op-name [this] opname)))
  (^BinaryPredicate [ifn]
   (ifn->binary-predicate ifn :_unnamed)))


(defn ->predicate
  (^BinaryPredicate [item opname]
   (cond
     (instance? BinaryPredicate item) item
     (instance? Comparator item)
     (let [^Comparator item item]
       (reify
         BinaryPredicates$ObjectBinaryPredicate
         (binaryObject [this lhs rhs]
           (== 0 (.compare item lhs rhs)))
         dtype-proto/POperator
         (op-name [this] opname)))
     (instance? java.util.function.BiPredicate item)
     (let [^java.util.function.BiPredicate item item]
       (reify
         BinaryPredicates$ObjectBinaryPredicate
         (binaryObject [this lhs rhs]
           (.test item lhs rhs))
         dtype-proto/POperator
         (op-name [this] opname)))
     (instance? IFn item) (ifn->binary-predicate item opname)))
  (^BinaryPredicate [item] (->predicate item :_unnamed)))


(defn reader
  ^Buffer [pred lhs-rdr rhs-rdr]
  (let [pred (->predicate pred)
        op-dtype (casting/simple-operation-space
                  (dtype-base/elemwise-datatype lhs-rdr)
                  (dtype-base/elemwise-datatype rhs-rdr))
        lhs-rdr (dtype-base/->reader lhs-rdr op-dtype)
        rhs-rdr (dtype-base/->reader rhs-rdr op-dtype)]
    (when-not (== (.lsize lhs-rdr)
                  (.lsize rhs-rdr))
      (errors/throwf "lhs size (%d), rhs size (%d) mismatch"
                     (.lsize lhs-rdr)
                     (.lsize rhs-rdr)))
    (case op-dtype
      :boolean
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readBoolean [rdr idx]
          (.binaryBoolean pred
                          (.readBoolean lhs-rdr idx)
                          (.readBoolean rhs-rdr idx))))
      :int64
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readBoolean [rdr idx]
          (.binaryLong pred
                       (.readLong lhs-rdr idx)
                       (.readLong rhs-rdr idx))))
      :float64
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readBoolean [rdr idx]
          (.binaryDouble pred
                       (.readDouble lhs-rdr idx)
                       (.readDouble rhs-rdr idx))))
      (reify ObjectReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readObject [rdr idx]
          (.binaryObject pred
                         (.readObject lhs-rdr idx)
                         (.readObject rhs-rdr idx)))))))


(defn iterable
  [pred lhs rhs]
  (let [pred (->predicate pred)
        lhs (dtype-base/->iterable lhs)
        rhs (dtype-base/->iterable rhs)]
    (dispatch/typed-map-2 pred :boolean lhs rhs)))


(defmacro make-boolean-predicate
  ([_opname op]
   `(reify BinaryPredicates$BooleanBinaryPredicate
      (binaryBoolean [this ~'x ~'y]
        (boolean ~op))))
  ([op]
   `(make-boolean-predicate ~op :_unnamed)))


(defmacro make-numeric-binary-predicate
  ([opname op obj-op]
   `(reify
      BinaryPredicates$ObjectBinaryPredicate
      (binaryByte [this ~'x ~'y] ~op)
      (binaryShort [this ~'x ~'y] ~op)
      (binaryInt [this ~'x ~'y] ~op)
      (binaryLong [this ~'x ~'y] ~op)
      (binaryFloat [this ~'x ~'y] ~op)
      (binaryDouble [this ~'x ~'y] ~op)
      (binaryObject [this ~'x ~'y]
        ~obj-op)
      dtype-proto/POperator
      (op-name [this] ~opname)))
  ([op]
   `(make-numeric-binary-predicate  :_unnamed ~op)))


(def builtin-ops
  {:tech.numerics/and (make-boolean-predicate :boolean (boolean (and x y)))
   :tech.numerics/or (make-boolean-predicate :boolean (boolean (or x y)))
   :tech.numerics/eq
   (reify
     BinaryPredicates$ObjectBinaryPredicate
     (binaryBoolean [this lhs rhs] (= lhs rhs))
     (binaryByte [this lhs rhs] (== lhs rhs))
     (binaryShort [this lhs rhs] (== lhs rhs))
     (binaryChar [this lhs rhs] (= lhs rhs))
     (binaryInt [this lhs rhs] (== lhs rhs))
     (binaryLong [this lhs rhs] (== lhs rhs))
     (binaryFloat [this lhs rhs]
       (if (Float/isNaN lhs)
         (Float/isNaN rhs)
         (pmath/== lhs rhs)))
     (binaryDouble [this lhs rhs]
       (if (Double/isNaN lhs)
         (Double/isNaN rhs)
         (pmath/== lhs rhs)))
     (binaryObject [this lhs rhs]
       (if (and (number? lhs) (number? rhs))
         (if (and (integer? lhs) (integer? rhs))
           (.binaryLong this lhs rhs)
           (.binaryDouble this lhs rhs))
         (if lhs
           (.equals ^Object lhs rhs)
           (= lhs rhs))))
     dtype-proto/POperator
     (op-name [this] :eq))
   :tech.numerics/not-eq
   (reify
     BinaryPredicates$ObjectBinaryPredicate
     (binaryBoolean [this lhs rhs] (not= lhs rhs))
     (binaryByte [this lhs rhs] (not= lhs rhs))
     (binaryShort [this lhs rhs] (not= lhs rhs))
     (binaryChar [this lhs rhs] (not= lhs rhs))
     (binaryInt [this lhs rhs] (not= lhs rhs))
     (binaryLong [this lhs rhs] (not= lhs rhs))
     (binaryFloat [this lhs rhs] (not (Precision/equalsIncludingNaN lhs rhs)))
     (binaryDouble [this lhs rhs] (not (Precision/equalsIncludingNaN lhs rhs)))
     (binaryObject [this lhs rhs]
       (if lhs
         (not (.equals ^Object lhs rhs))
         (= lhs rhs)))
     dtype-proto/POperator
     (op-name [this] :eq))

   :tech.numerics/> (make-numeric-binary-predicate
                     :> (pmath/> x y)
                     (let [comp-val (long (if (instance? Comparable x)
                                            (.compareTo ^Comparable x y)
                                            (compare x y)))]
                       (pmath/> comp-val 0)))
   :tech.numerics/>= (make-numeric-binary-predicate
                      :>= (pmath/>= x y)
                      (let [comp-val (long (if (instance? Comparable x)
                                             (.compareTo ^Comparable x y)
                                             (compare x y)))]
                        (pmath/>= comp-val 0)))
   :tech.numerics/< (make-numeric-binary-predicate
                     :< (pmath/< x y)
                     (let [comp-val (long (if (instance? Comparable x)
                                            (.compareTo ^Comparable x y)
                                            (compare x y)))]
                       (pmath/< comp-val 0)))
   :tech.numerics/<= (make-numeric-binary-predicate
                      :<= (pmath/<= x y)
                      (let [comp-val (long (if (instance? Comparable x)
                                             (.compareTo ^Comparable x y)
                                             (compare x y)))]
                        (pmath/<= comp-val 0)))})

(defn builtin
  "Return the builtin binary predicate for the given keyword or error."
  ^BinaryPredicate [kwd]
  (if-let [retval (builtin-ops kwd)]
    retval
    (throw (Exception. (format "Failed to bind binary predicate: %s" kwd)))))


(defn unary-pred
  "Create a unary predicate from a binary predicate and a constant passed into
  the left position.  This means a statement such as:
  ```clojure
  ((unary-pred 5.0 :tech.numerics/>) 6.0)
  ```
  yeilds false.

  Operation will happen in the numeric space of the constant.  Integers will happen
  in :int64, floating point comparisons will happen in :float64, booleans in :boolean and
  anything else will happen in object space."
  ^UnaryPredicate [constant kwd]
  (let [^BinaryPredicate pred (if (keyword? kwd)
                                (builtin kwd)
                                (->predicate kwd))]
    (let [c-dtype (dtype-base/datatype constant)]
      (case (casting/simple-operation-space c-dtype)
        :int64 (let [lval (unchecked-long constant)]
                 (reify UnaryPredicates$LongUnaryPredicate
                   (unaryLong [this arg]
                     (.binaryLong pred lval arg))))
        :float64 (let [dval (unchecked-double constant)]
                   (reify UnaryPredicates$DoubleUnaryPredicate
                     (unaryDouble [this arg]
                       (.binaryDouble pred dval arg))))
        :boolean (let [bval (boolean constant)]
                   (reify UnaryPredicates$BooleanUnaryPredicate
                     (unaryBoolean [this arg]
                       (.binaryBoolean pred bval arg))))
        (reify UnaryPredicates$ObjectUnaryPredicate
          (unaryObject [this arg]
            (= constant arg)))))))
