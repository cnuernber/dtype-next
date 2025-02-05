(ns tech.v3.datatype.unary-op
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.double-ops :refer [get-significand]]
            [ham-fisted.api :as hamf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.reduce :as hamf-rf])
  (:import [tech.v3.datatype LongReader DoubleReader ObjectReader
            UnaryOperator Buffer
            UnaryOperators$DoubleUnaryOperator
            UnaryOperators$LongUnaryOperator]
           [clojure.lang IFn IFn$LL IFn$DD IFn$OLO IFn$ODO IFn$OL IFn$OD IFn$DL IFn$LD]
           [java.util.function DoubleUnaryOperator LongUnaryOperator]
           [ham_fisted Casts Reductions IFnDef$OLO IFnDef$ODO]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ->operator
  (^UnaryOperator [item _opname]
   (cond
     (instance? UnaryOperator item)
     item
     (instance? DoubleUnaryOperator item)
     (with-meta (reify UnaryOperators$DoubleUnaryOperator
                  (unaryDouble [this arg]
                    (.applyAsDouble ^DoubleUnaryOperator item arg)))
       {:result-space :float64 :operation-space :float64})
     (instance? LongUnaryOperator item)
     (with-meta
       (reify UnaryOperators$LongUnaryOperator
         (unaryLong [this arg]
           (.applyAsLong ^LongUnaryOperator item arg)))
       {:result-space :int64 :operation-space :int64})
     (instance? java.util.function.UnaryOperator item)
     (with-meta
       (reify UnaryOperator
         (unaryObject [this arg]
           (.apply ^java.util.function.UnaryOperator item arg)))
       {:result-space :object :operation-space :object})
     ;;Five cases we care about -
     ;;long->long
     ;;double->double
     ;;obj->long
     ;;obj->double
     ;;obj->obj
     (instance? IFn$LL item)
     (with-meta
       (reify UnaryOperators$LongUnaryOperator
         (unaryLong [_t v]
           (.invokePrim ^IFn$LL item v)))
       {:result-space :int64 :operation-space :int64})
     (instance? IFn$DD item)
     (with-meta
       (reify UnaryOperators$DoubleUnaryOperator
         (unaryDouble [_t v]
           (.invokePrim ^IFn$DD item v)))
       {:result-space :float64 :operation-space :float64})
     (instance? IFn$OL item)
     (with-meta
       (reify UnaryOperator
         (unaryObject [_t v] (.invokePrim ^IFn$OL item v))
         (unaryObjLong [_t v] (.invokePrim ^IFn$OL item v)))
       {:result-space :int64 :operation-space :object})
     (instance? IFn$OD item)
     (with-meta
       (reify UnaryOperator
         (unaryObject [_t v] (.invokePrim ^IFn$OD item v))
         (unaryObjDouble [_t v] (.invokePrim ^IFn$OD item v)))
       {:result-space :float64 :operation-space :object})
     :else
     (with-meta
       (reify UnaryOperator
         (unaryObject [this arg]
           (item arg)))
       {:result-space :object :operation-space :object})))
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

(declare reader)


(defn- apply-nested-unary
  [lhs unary-op1 op-space1 unary-op2 op-space2 res-dtype]
  (let [unary-op1 (->operator unary-op1)
        unary-op2 (->operator unary-op2)]
    (->> (with-meta (case op-space2
                      :int64
                      (case op-space1
                        :int64 (reify UnaryOperators$LongUnaryOperator
                                 (unaryLong [this v]
                                   (->> (.unaryLong unary-op1 v)
                                        (.unaryLong unary-op2))))
                        :float64 (hamf-fn/double->long
                                  v (->> (.unaryDouble unary-op1 v)
                                         (Casts/longCast)
                                         (.unaryLong unary-op2)))
                        (hamf-fn/obj->long
                         v (->> (.unaryObject unary-op1 v)
                                (Casts/longCast)
                                (.unaryLong unary-op2))))
                      :float64
                      (case op-space1
                        :int64 (hamf-fn/long->double
                                v (->> (.unaryLong unary-op1 v)
                                       (double)
                                       (.unaryDouble unary-op2)))
                        :float64 (reify UnaryOperators$DoubleUnaryOperator
                                   (unaryDouble [this v]
                                     (->> (.unaryDouble unary-op1 v)
                                          (.unaryDouble unary-op2))))
                        (hamf-fn/obj->double
                         v (->> (.unaryObject unary-op1 v)
                                (Casts/doubleCast)
                                (.unaryDouble unary-op2))))
                      (case op-space1
                        :int64 (reify UnaryOperator
                                 (unaryObject [this v ]
                                   (->> (Casts/longCast v)
                                        (.unaryLong unary-op1)
                                        (.unaryObject unary-op2))))
                        :float64 (reify UnaryOperator
                                   (unaryObject [this v]
                                     (->> (Casts/doubleCast v)
                                          (.unaryDouble unary-op1)
                                          (.unaryObject unary-op2))))
                        (reify UnaryOperator
                          (unaryObject [this v]
                            (->> (.unaryObject unary-op1 v)
                                 (.unaryObject unary-op2))))))
           {:operation-space op-space2
            :result-space res-dtype})
     (dtype-proto/apply-unary-op lhs res-dtype))))


(extend-type Object
  dtype-proto/PApplyUnary
  (apply-unary-op [lhs res-dtype unary-op]
    (let [unary-op (->operator unary-op)
          input-dtype (dtype-base/elemwise-datatype lhs)
          op-space (if-let [un-op-space (get (meta unary-op) :operation-space)]
                     (casting/simple-operation-space un-op-space input-dtype)
                     (casting/simple-operation-space res-dtype input-dtype))
          lhs (dtype-base/->reader lhs op-space)
          n-elems (.lsize lhs)
          nested-fn (fn [res-dtype2 unary-op2]
                      (apply-nested-unary lhs unary-op op-space
                                          unary-op2 (casting/simple-operation-space
                                                     op-space res-dtype2)
                                          res-dtype2))]
     ;;Five common cases -
     ;;long->long
     ;;double->double
     ;;obj->long
     ;;obj->double
     ;;obj->obj
     (case op-space
       :int64
       (let [wrap-rfn
             (fn [rfn]
               (if (instance? IFn$OLO rfn)
                 (reify IFnDef$OLO
                   (invokePrim [f acc v]
                     (.invokePrim ^IFn$OLO rfn acc (.unaryLong unary-op v))))
                 (reify IFnDef$OLO
                   (invokePrim [f acc v]
                     (rfn acc (.unaryLong unary-op v))))))]
         (reify
           LongReader
           (elemwiseDatatype [rdr] res-dtype)
           (subBuffer [rdr sidx eidx] (reader unary-op res-dtype (.subBuffer lhs sidx eidx)))
           (lsize [rdr] n-elems)
           (readLong [rdr idx] (.unaryLong unary-op (.readLong lhs idx)))
           (reduce [rdr rfn init-val]
             (reduce (wrap-rfn rfn) init-val lhs))
           (parallelReduction [rdr initValFn rfn mergeFn options]
             (Reductions/parallelReduction initValFn (wrap-rfn rfn) mergeFn lhs options))
           dtype-proto/PApplyUnary
           (apply-unary-op [rdr res-dtype2 unary-op2] (nested-fn res-dtype2 unary-op2))))
       :float64
       (let [wrap-rfn
             (fn [rfn]
               (cond
                 (instance? IFn$ODO rfn)
                 (hamf-rf/double-accumulator
                  acc v (.invokePrim ^IFn$ODO rfn acc (.unaryDouble unary-op v)))
                 (instance? IFn$OLO rfn)
                 (hamf-rf/double-accumulator
                  acc v (.invokePrim ^IFn$OLO rfn acc
                                     (Casts/longCast (.unaryDouble unary-op v))))
                 :else
                 (hamf-rf/double-accumulator acc v (rfn acc (.unaryDouble unary-op v)))))]
         (reify DoubleReader
           (elemwiseDatatype [rdr] res-dtype)
           (lsize [rdr] n-elems)
           (subBuffer [rdr sidx eidx] (reader unary-op res-dtype (.subBuffer lhs sidx eidx)))
           (readDouble [rdr idx] (.unaryDouble unary-op (.readDouble lhs idx)))
           (reduce [rdr rfn init-val]
             (reduce (wrap-rfn rfn) init-val lhs))
           (parallelReduction [rdr initValFn rfn mergeFn options]
             (Reductions/parallelReduction initValFn (wrap-rfn rfn) mergeFn lhs options))
           dtype-proto/PApplyUnary
           (apply-unary-op [rdr res-dtype2 unary-op2] (nested-fn res-dtype2 unary-op2))))
       (case (casting/simple-operation-space res-dtype)
         ;;obj->long transformation
         :int64
         (let [wrap-rfn (fn [rfn]
                          (if (instance? IFn$OLO rfn)
                            (fn [acc v]
                              (.invokePrim ^IFn$OLO rfn acc (.unaryObjLong unary-op v)))
                            (fn [acc v]
                              (rfn acc (.unaryObjLong unary-op v)))))]
           (reify
             LongReader
             (elemwiseDatatype [rdr] res-dtype)
             (subBuffer [rdr sidx eidx] (reader unary-op res-dtype (.subBuffer lhs sidx eidx)))
             (lsize [rdr] n-elems)
             (readLong [rdr idx] (.unaryObjLong unary-op (.readObject lhs idx)))
             (reduce [rdr rfn init-val]
               (reduce (wrap-rfn rfn) init-val lhs))
             (parallelReduction [rdr initValFn rfn mergeFn options]
               (Reductions/parallelReduction initValFn (wrap-rfn rfn) mergeFn lhs options))
             dtype-proto/PApplyUnary
             (apply-unary-op [rdr res-dtype2 unary-op2] (nested-fn res-dtype2 unary-op2))))
         :float64
         (let [wrap-rfn (fn [rfn]
                          (if (instance? IFn$ODO rfn)
                            (fn [acc v]
                              (.invokePrim ^IFn$ODO rfn acc (.unaryObjDouble unary-op v)))
                            (fn [acc v]
                              (rfn acc (.unaryObjDouble unary-op v)))))]
           (reify
             DoubleReader
             (elemwiseDatatype [rdr] res-dtype)
             (subBuffer [rdr sidx eidx] (reader unary-op res-dtype (.subBuffer lhs sidx eidx)))
             (lsize [rdr] n-elems)
             (readDouble [rdr idx] (.unaryObjDouble unary-op (.readObject lhs idx)))
             (reduce [rdr rfn init-val]
               (reduce (wrap-rfn rfn) init-val lhs))
             (parallelReduction [rdr initValFn rfn mergeFn options]
               (Reductions/parallelReduction initValFn (wrap-rfn rfn) mergeFn lhs options))
             dtype-proto/PApplyUnary
             (apply-unary-op [rdr res-dtype2 unary-op2] (nested-fn res-dtype2 unary-op2))))
         ;;Default obj->obj transformation
         (let [wrap-rfn (fn [rfn] (fn [acc v] (rfn acc (.unaryObject unary-op v))))]
           (reify ObjectReader
             (elemwiseDatatype [rdr] res-dtype)
             (subBuffer [rdr sidx eidx] (reader unary-op res-dtype (.subBuffer lhs sidx eidx)))
             (lsize [rdr] n-elems)
             (readObject [rdr idx] (.unaryObject unary-op (.readObject lhs idx)))
             (reduce [rdr rfn init-val]
               (reduce (wrap-rfn rfn) init-val lhs))
             (parallelReduction [rdr initValFn rfn mergeFn options]
               (Reductions/parallelReduction initValFn (wrap-rfn rfn)
                                             mergeFn lhs options))
             dtype-proto/PApplyUnary
             (apply-unary-op [rdr res-dtype2 unary-op2]
               (nested-fn res-dtype2 unary-op2)))))))))

(defn reader
  ([unary-op res-dtype lhs]
   (dtype-proto/apply-unary-op lhs res-dtype unary-op))
  ([unary-op lhs]
   (let [lhs-dtype (dtype-base/elemwise-datatype lhs)
         op-meta (meta unary-op)
         res-dtype (if-let [rs (get op-meta :result-space)]
                     rs
                     (if-let [os (get op-meta :operation-space)]
                       (casting/simple-operation-space os lhs-dtype)
                       (cond
                         (instance? UnaryOperator unary-op)
                         :object
                         (or (instance? IFn$OL unary-op)
                             (instance? IFn$LL unary-op)
                             (instance? IFn$DL unary-op))
                         :int64
                         (or (instance? IFn$OD unary-op)
                             (instance? IFn$DD unary-op)
                             (instance? IFn$LD unary-op))
                         :float64
                         :else
                         :object)))]
     (dtype-proto/apply-unary-op lhs res-dtype unary-op))))


(defmacro make-double-unary-op
  [opname opcode]
  `(-> (reify
         dtype-proto/POperator
         (op-name [item#] ~opname)
         UnaryOperators$DoubleUnaryOperator
         (unaryDouble [this# ~'x] ~opcode))
       (vary-meta assoc :operation-space :float64 :result-space :float64)))


(defmacro make-numeric-object-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     UnaryOperator
     (unaryLong [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# ~'x]
       (if (casting/integer-type? (dtype-base/elemwise-datatype ~'x))
         (let [~'x (Casts/longCast ~'x)]
           ~opcode)
         (let [~'x (Casts/doubleCast ~'x)]
           ~opcode)))))


(defmacro make-float-double-unary-op
  [opname opcode]
  `(-> (reify
         dtype-proto/POperator
         (op-name [item#] ~opname)
         UnaryOperators$DoubleUnaryOperator
         (unaryDouble [this# ~'x] ~opcode))
       (vary-meta assoc :operation-space :float32 :result-space :float64)))


(defmacro make-numeric-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     UnaryOperator
     (unaryLong [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# x#] (.unaryDouble this# x#))))


(defmacro make-long-unary-op
  [opname opcode]
  `(-> (reify
         dtype-proto/POperator
         (op-name [item#] ~opname)
         UnaryOperators$LongUnaryOperator
         (unaryLong [this# ~'x] ~opcode)
         (unaryDouble [this# ~'x]
           (throw (Exception. (format "op %s not defined for double" ~opname))))
         (unaryObject [this# x#] (.unaryLong this# (Casts/longCast x#))))
       (vary-meta assoc :operation-space :int64 :result-space :int64)))


(defmacro make-all-datatype-unary-op
  [opname opcode]
  `(reify
     dtype-proto/POperator
     (op-name [item#] ~opname)
     UnaryOperator
     (unaryLong [this# ~'x] ~opcode)
     (unaryDouble [this# ~'x] ~opcode)
     (unaryObject [this# ~'x] ~opcode)))


(def builtin-ops
  (->> [(make-double-unary-op :tech.numerics/floor (Math/floor x))
        (make-double-unary-op :tech.numerics/ceil (Math/ceil x))
        (make-double-unary-op :tech.numerics/rint (Math/rint x))
        (make-double-unary-op :tech.numerics/get-significand (unchecked-double (get-significand x)))
        (make-numeric-object-unary-op :tech.numerics/- (- x))
        (make-numeric-object-unary-op :tech.numerics/min x)
        (make-numeric-object-unary-op :tech.numerics/max x)
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
