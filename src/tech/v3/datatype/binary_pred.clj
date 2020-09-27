(ns tech.v3.datatype.binary-pred
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.errors :as errors]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryPredicate Buffer
            BinaryPredicates$BooleanBinaryPredicate
            BinaryPredicates$DoubleBinaryPredicate
            BinaryPredicates$LongBinaryPredicate
            BinaryPredicates$ObjectBinaryPredicate
            BooleanReader LongReader DoubleReader ObjectReader]
           [clojure.lang IFn]
           [clojure.lang Numbers]
           [org.apache.commons.math3.util Precision]
           [java.time Instant LocalDate ZonedDateTime]
           [java.util Date]))

(set! *warn-on-reflection* true)


(defn ifn->binary-predicate
  (^BinaryPredicate [ifn opname]
   (when-not (instance? IFn ifn)
     (errors/throwf "Arg (%s) is not an instance of IFn" ifn))
   (reify
     BinaryPredicates$ObjectBinaryPredicate
     (binaryObject [this lhs rhs]
       (boolean (ifn lhs rhs)))
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
       (boolean (ifn lhs rhs)))
     dtype-proto/POperator
     (op-name [this] opname)))
  (^BinaryPredicate [ifn]
   (ifn->binary-predicate ifn :_unnamed)))


(defn ->predicate
  (^BinaryPredicate [item opname]
   (cond
     (instance? BinaryPredicate item) item
     (instance? IFn item) (ifn->binary-predicate item opname)
     (instance? BinaryPredicate item)
     (let [^BinaryPredicate item item]
       (reify
         BinaryPredicates$ObjectBinaryPredicate
         (binaryObject [this lhs rhs]
           (.test item lhs rhs))
         dtype-proto/POperator
         (op-name [this] opname)))))
  (^BinaryPredicate [item] (->predicate item :_unnamed)))


(defn reader
  ^Buffer [pred lhs-rdr rhs-rdr]
  (let [pred (->predicate pred)
        lhs-rdr (dtype-base/->reader lhs-rdr)
        rhs-rdr (dtype-base/->reader rhs-rdr)
        op-dtype (casting/widest-datatype (.elemwiseDatatype lhs-rdr)
                                          (.elemwiseDatatype rhs-rdr))]
    (when-not (== (.lsize lhs-rdr)
                  (.lsize rhs-rdr))
      (errors/throwf "lhs size (%d), rhs size (%d) mismatch"
                     (.lsize lhs-rdr)
                     (.lsize rhs-rdr)))
    (cond
      (= :boolean op-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readBoolean [rdr idx]
          (.binaryBoolean pred
                          (.readBoolean lhs-rdr idx)
                          (.readBoolean rhs-rdr idx))))
      (casting/integer-type? op-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readBoolean [rdr idx]
          (.binaryLong pred
                       (.readLong lhs-rdr idx)
                       (.readLong rhs-rdr idx))))
      (casting/float-type? op-dtype)
      (reify BooleanReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readBoolean [rdr idx]
          (.binaryDouble pred
                       (.readDouble lhs-rdr idx)
                       (.readDouble rhs-rdr idx))))
      :else
      (reify ObjectReader
        (lsize [rdr] (.lsize lhs-rdr))
        (readObject [rdr idx]
          (.binaryObject pred
                         (.readObject lhs-rdr idx)
                         (.readObject rhs-rdr idx)))))))


(defn iterable
  [pred lhs rhs]
  (let [pred (->predicate pred)
        lhs (dtype-base/ensure-iterable lhs)
        rhs (dtype-base/ensure-iterable rhs)]
    (dispatch/typed-map-2 pred :boolean lhs rhs)))


(defmacro make-boolean-predicate
  ([opname op]
   `(reify BinaryPredicates$BooleanBinaryPredicate
      (binaryBoolean [this ~'x ~'y]
        (boolean ~op))))
  ([op]
   `(make-boolean-predicate op :_unnamed)))


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
  {:and (make-boolean-predicate :boolean (boolean (and x y)))
   :or (make-boolean-predicate :boolean (boolean (or x y)))
   :eq
   (reify
     BinaryPredicates$ObjectBinaryPredicate
     (binaryBoolean [this lhs rhs] (= lhs rhs))
     (binaryByte [this lhs rhs] (== lhs rhs))
     (binaryShort [this lhs rhs] (== lhs rhs))
     (binaryChar [this lhs rhs] (= lhs rhs))
     (binaryInt [this lhs rhs] (== lhs rhs))
     (binaryLong [this lhs rhs] (== lhs rhs))
     (binaryFloat [this lhs rhs] (Precision/equalsIncludingNaN lhs rhs))
     (binaryDouble [this lhs rhs] (Precision/equalsIncludingNaN lhs rhs))
     (binaryObject [this lhs rhs] (= lhs rhs))
     dtype-proto/POperator
     (op-name [this] :eq))
   :not-eq
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
     (binaryObject [this lhs rhs] (not= lhs rhs))
     dtype-proto/POperator
     (op-name [this] :eq))

   :> (make-numeric-binary-predicate :> (pmath/> x y)
                                     (cond
                                       (and (instance? Instant x)
                                            (instance? Instant y))
                                       (.isAfter ^Instant x ^Instant y)
                                       (and (instance? LocalDate x)
                                            (instance? LocalDate y))
                                       (.isAfter ^LocalDate x ^LocalDate y)
                                       (and (instance? ZonedDateTime x)
                                            (instance? ZonedDateTime y))
                                       (.isAfter ^ZonedDateTime x ^ZonedDateTime y)
                                       :else
                                       (Numbers/gt x y)))
   :>= (make-numeric-binary-predicate :>= (pmath/>= x y)
                                      (or (.equals ^Object x y)
                                          (cond
                                            (and (instance? Instant x)
                                                 (instance? Instant y))
                                            (.isAfter ^Instant x ^Instant y)
                                            (and (instance? LocalDate x)
                                                 (instance? LocalDate y))
                                            (.isAfter ^LocalDate x ^LocalDate y)
                                            (and (instance? ZonedDateTime x)
                                                 (instance? ZonedDateTime y))
                                            (.isAfter ^ZonedDateTime x ^ZonedDateTime y)
                                            :else
                                            (Numbers/gt x y))))
   :< (make-numeric-binary-predicate :< (pmath/< x y)
                                     (cond
                                       (and (instance? Instant x)
                                            (instance? Instant y))
                                       (.isBefore ^Instant x ^Instant y)
                                       (and (instance? LocalDate x)
                                            (instance? LocalDate y))
                                       (.isBefore ^LocalDate x ^LocalDate y)
                                       (and (instance? ZonedDateTime x)
                                            (instance? ZonedDateTime y))
                                       (.isBefore ^ZonedDateTime x ^ZonedDateTime y)
                                       :else
                                       (Numbers/lt x y)))
   :<= (make-numeric-binary-predicate :<= (pmath/<= x y)
                                      (or (.equals ^Object x y)
                                          (cond
                                            (and (instance? Instant x)
                                                 (instance? Instant y))
                                            (.isBefore ^Instant x ^Instant y)
                                            (and (instance? LocalDate x)
                                                 (instance? LocalDate y))
                                            (.isBefore ^LocalDate x ^LocalDate y)
                                            (and (instance? ZonedDateTime x)
                                                 (instance? ZonedDateTime y))
                                            (.isBefore ^ZonedDateTime x ^ZonedDateTime y)
                                            :else
                                            (Numbers/lte x y))))})
