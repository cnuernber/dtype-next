(ns tech.v3.datatype.dispatch-perf-tests
  (:require [tech.v3.datatype :as dt]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.op-dispatch :refer [dispatch-binary-op dispatch-unary-op]]
            [ham-fisted.api :as hamf]
            [ham-fisted.language :refer [cond] :as hamf-language])
  (:import [tech.v3.datatype Buffer UnaryOperator BinaryOperator
            UnaryOperators$DoubleUnaryOperator UnaryOperators$LongUnaryOperator
            BinaryOperators$DoubleBinaryOperator BinaryOperators$LongBinaryOperator
            LongReader DoubleReader ObjectReader]
           [ham_fisted Casts])
  (:refer-clojure :exclude [cond]))

(def plus-op (binary-op/builtin-ops :tech.numerics/+))
(defn fast-+
  [a b]
  (dispatch-binary-op plus-op a b))

(def rem-op (with-meta
              (reify BinaryOperators$LongBinaryOperator
                (binaryLong [this a b] (rem a b)))
              {:operation-space :int64}))

(defn fast-rem
  [a b]
  (dispatch-binary-op rem-op :int64 :int64 a b))

(def log1p-op (with-meta (reify UnaryOperators$DoubleUnaryOperator
                           (unaryDouble [this x] (Math/log1p x)))
                {:operation-space :float64}))

(defn fast-log1p
  [x]
  (dispatch-unary-op log1p-op :float64 x))

(defn fast-double
  [x]
  (dispatch-unary-op (reify UnaryOperators$DoubleUnaryOperator
                       (unaryDouble [this x] x)
                       (unaryObjDouble [this x] (Casts/doubleCast x)))
                     :object :float64 x))

(defn fast-long
  [x]
  (dispatch-unary-op (reify UnaryOperators$LongUnaryOperator
                       (unaryLong [this x] x)
                       (unaryObjLong [this x] (Casts/longCast x)))
                     :object :int64 x))

(comment
  (do
    (require '[clj-async-profiler.core :as prof])
    (prof/serve-ui 8080))

  (def data (long-array (range 10)))

  (do
    (require '[criterium.core :as crit])
    (crit/quick-bench (hamf/lsum (dfn/+ data (dfn/+ data (hamf/lsum (dfn/+ data 10)))))))

  ;;848 ns
  (crit/quick-bench (hamf/lsum (fast-+ data (fast-+ data (hamf/lsum (fast-+ data 10))))))
  

  (prof/profile {:interval 10000}
    (dotimes [idx 1000000]
      (hamf/lsum (fast-+ data (fast-+ data (hamf/lsum (fast-+ data 10)))))))

  (let [cc (constantly 4)]
    (prof/profile {:event :alloc}
      (dotimes [idx 1000000]
        (cc :a))))
  )
