(ns tech.v3.datatype.binary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.errors :as errors]
            [clj-commons.primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryPredicate Buffer
            BinaryPredicates$LongBinaryPredicate
            BinaryPredicates$DoubleBinaryPredicate
            BooleanReader ObjectReader
            UnaryPredicate
            UnaryPredicates$LongUnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate
            UnaryPredicates$ObjectUnaryPredicate]
           [clojure.lang IFn IFn$LLO IFn$DDO]
           [org.apache.commons.math3.util Precision]
           [java.util Comparator]
           [ham_fisted Casts]))

(set! *warn-on-reflection* true)


(defn ifn->binary-predicate
  (^BinaryPredicate [ifn opname]
   (if (instance? BinaryPredicate ifn)
     ifn
     (cond
       (instance? IFn$LLO ifn)
       (reify
         BinaryPredicates$LongBinaryPredicate
         (binaryLong [this lhs rhs]
           (Casts/booleanCast (.invokePrim ^IFn$LLO ifn lhs rhs)))
         dtype-proto/POperator
         (op-name [this] opname))
       (instance? IFn$DDO ifn)
       (reify
         BinaryPredicates$DoubleBinaryPredicate
         (binaryDouble [this lhs rhs]
           (Casts/booleanCast (.invokePrim ^IFn$DDO ifn lhs rhs)))
         dtype-proto/POperator
         (op-name [this] opname))
       :else
       (reify
         BinaryPredicate
         (binaryObject [this lhs rhs]
           (Casts/booleanCast (ifn lhs rhs)))
         dtype-proto/POperator
         (op-name [this] opname)))))
  (^BinaryPredicate [ifn]
   (ifn->binary-predicate ifn :_unnamed)))


(defn ->predicate
  (^BinaryPredicate [item opname]
   (cond
     (instance? BinaryPredicate item) item
     (instance? Comparator item)
     (let [^Comparator item item]
       (reify
         BinaryPredicate
         (binaryObject [this lhs rhs]
           (== 0 (.compare item lhs rhs)))
         dtype-proto/POperator
         (op-name [this] opname)))
     (instance? java.util.function.BiPredicate item)
     (let [^java.util.function.BiPredicate item item]
       (reify
         BinaryPredicate
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
      :int64
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readObject [rdr idx]
          (.binaryLong pred
                       (.readLong lhs-rdr idx)
                       (.readLong rhs-rdr idx))))
      :float64
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readObject [rdr idx]
          (.binaryDouble pred
                       (.readDouble lhs-rdr idx)
                       (.readDouble rhs-rdr idx))))
      (reify BooleanReader
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
   `(reify BinaryPredicate
      (binaryObject [this ~'x ~'y]
        (Casts/booleanCast ~op))))
  ([op]
   `(make-boolean-predicate ~op :_unnamed)))


(defmacro make-numeric-binary-predicate
  ([opname op obj-op]
   `(reify
      BinaryPredicate
      (binaryLong [this ~'x ~'y] ~op)
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
     BinaryPredicate
     (binaryLong [this lhs rhs] (== lhs rhs))
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
     BinaryPredicate
     (binaryLong [this lhs rhs] (not= lhs rhs))
     (binaryDouble [this lhs rhs] (not (Precision/equalsIncludingNaN lhs rhs)))
     (binaryObject [this lhs rhs]
       (if lhs
         (not (.equals ^Object lhs rhs))
         (= lhs rhs)))
     dtype-proto/POperator
     (op-name [this] :eq))

   :tech.numerics/> (make-numeric-binary-predicate
                     :> (pmath/> x y)
                     (clojure.core/> x y))
   :tech.numerics/>= (make-numeric-binary-predicate
                      :>= (pmath/>= x y)
                      (clojure.core/>= x y))
   :tech.numerics/< (make-numeric-binary-predicate
                     :< (pmath/< x y)
                     (clojure.core/< x y))
   :tech.numerics/<= (make-numeric-binary-predicate
                      :<= (pmath/<= x y)
                      (clojure.core/<= x y))
   :tech.numerics/bit-test (reify
                             BinaryPredicate
                             (binaryLong [this lhs rhs]
                               (bit-test lhs rhs))
                             (binaryDouble [this lhs rhs]
                               (.binaryLong this (Casts/longCast lhs) (Casts/longCast rhs)))
                             (binaryObject [this lhs rhs]
                               (.binaryLong this (Casts/longCast lhs) (Casts/longCast rhs)))
                             dtype-proto/POperator
                             (op-name [this] :bit-test))})

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
                   (reify UnaryPredicate
                     (unaryObject [this arg]
                       (.binaryObject pred bval arg))))
        (reify UnaryPredicates$ObjectUnaryPredicate
          (unaryObject [this arg]
            (= constant arg)))))))
