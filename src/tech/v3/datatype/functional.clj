(ns tech.v3.datatype.functional
  (:require [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.const-reader :refer [const-reader]]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1
                                               vectorized-dispatch-2]
             :as dispatch]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.rolling]
            [primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype BinaryOperator PrimitiveReader
            LongReader DoubleReader ObjectReader
            UnaryOperator BinaryOperator
            UnaryPredicate BinaryPredicate])
  (:refer-clojure :exclude [+ - / *
                            <= < >= >
                            identity
                            min max
                            bit-xor bit-and bit-and-not bit-not bit-set bit-test
                            bit-or bit-flip bit-clear
                            bit-shift-left bit-shift-right unsigned-bit-shift-right
                            quot rem cast not and or
                            neg? even? zero? odd? pos?]))


(defn unary-operator
  ^UnaryOperator [op-kwd]
  (if-let [retval (get unary-op/builtin-ops op-kwd)]
    retval
    (errors/throwf "Unrecognized unary operator: %s" op-kwd)))


(defn binary-operator
  ^BinaryOperator [op-kwd]
  (if-let [retval (get binary-op/builtin-ops op-kwd)]
    retval
    (errors/throwf "Unrecognized binary operator: %s" op-kwd)))


(defn unary-predicate
  ^UnaryPredicate [op-kwd]
  (if-let [retval (get unary-pred/builtin-ops op-kwd)]
    retval
    (errors/throwf "Unrecognized unary predicate: %s" op-kwd)))


(defn binary-predicate
  ^BinaryPredicate [op-kwd]
  (if-let [retval (get binary-pred/builtin-ops op-kwd)]
    retval
    (errors/throwf "Unrecognized binary predicate: %s" op-kwd)))


(defmacro ^:private implement-arithmetic-operations
  []
  (let [binary-ops (set (keys binary-op/builtin-ops))
        unary-ops (set (keys unary-op/builtin-ops))
        dual-ops (set/intersection binary-ops unary-ops)
        unary-ops (set/difference unary-ops dual-ops)]
    `(do
       ~@(->>
          unary-ops
          (map
           (fn [opname]
             (let [op (unary-op/builtin-ops opname)
                   op-meta (meta op)
                   op-sym (vary-meta (symbol (name opname))
                                     merge op-meta)]
               `(defn ~(with-meta op-sym
                         {:unary-operator opname})
                  [~'x]
                  (vectorized-dispatch-1
                   ~'x
                   (unary-op/builtin-ops ~opname)
                   ;;the default iterable application is fine.
                   nil
                   #(unary-op/reader
                     (unary-op/builtin-ops ~opname)
                     %1
                     %2)
                   ~op-meta))))))
       ~@(->>
          binary-ops
          (map
           (fn [opname]
             (let [op (binary-op/builtin-ops opname)
                   op-meta (meta op)
                   op-sym (vary-meta (symbol (name opname))
                                     merge op-meta)
                   dual-op? (dual-ops opname)]
               (if dual-op?
                 `(defn ~(with-meta op-sym
                           {:binary-operator opname})
                    ([~'x]
                     (vectorized-dispatch-1
                      ~'x
                      (unary-op/builtin-ops ~opname)
                      nil
                      #(unary-op/reader
                        (unary-op/builtin-ops ~opname)
                        %1
                        %2)
                      ~op-meta))
                    ([~'x ~'y]
                     (vectorized-dispatch-2
                      ~'x ~'y
                      (binary-op/builtin-ops ~opname)
                      #(binary-op/reader
                        (binary-op/builtin-ops ~opname)
                        %1 %2 %3)
                      ~op-meta)))
                 `(defn ~(with-meta op-sym
                           {:binary-operator opname})
                    [~'x ~'y]
                    (vectorized-dispatch-2
                     ~'x ~'y
                     (binary-op/builtin-ops ~opname)
                     #(binary-op/reader
                       (binary-op/builtin-ops ~opname)
                       %1 %2 %3)
                     ~op-meta))))))))))


(implement-arithmetic-operations)


(defmacro ^:private implement-unary-predicates
  []
  `(do
     ~@(->> unary-pred/builtin-ops
            (map (fn [[k v]]
                   (let [fn-symbol (symbol (name k))]
                     `(let [v# (unary-pred/builtin-ops ~k)]
                        (defn ~(with-meta fn-symbol
                                 {:unary-predicate k})
                          [~'arg]
                          (vectorized-dispatch-1
                           v#
                           (fn [dtype# item#] (unary-pred/iterable v# item#))
                           (fn [dtype# item#] (unary-pred/reader v# item#))
                           ~'arg)))))))))


(implement-unary-predicates)


(defmacro ^:private implement-binary-predicates
  []
  `(do
     ~@(->> binary-pred/builtin-ops
            (map (fn [[k v]]
                   (let [fn-symbol (symbol (name k))]
                     `(let [v# (binary-pred/builtin-ops ~k)]
                        (defn ~(with-meta fn-symbol
                                 {:binary-predicate k})
                          [~'lhs ~'rhs]
                          (vectorized-dispatch-2
                           v#
                           (fn [op-dtype# lhs# rhs#]
                             (binary-pred/iterable v# lhs# rhs#))
                           (fn [op-dtype# lhs-rdr# rhs-rdr#]
                             (binary-pred/reader v# lhs-rdr# rhs-rdr#))
                           ~'lhs ~'rhs)))))))))


(implement-binary-predicates)

(defn- round-scalar
  ^long [^double arg]
  (Math/round arg))

(defn round
  "Returns a long reader but operates in double space."
  [arg]
  (vectorized-dispatch-1
   round-scalar
   (fn [dtype arg] (dispatch/typed-map-1 round-scalar :int64 arg))
   (fn [rdr op-dtype]
     (let [src-rdr (dtype-base/->reader rdr)]
       (reify LongReader
         (lsize [rdr] (.lsize src-rdr))
         (readLong [rdr idx]
           (Math/round (.readDouble src-rdr idx))))))
   arg))

;;Implement only reductions that we know we will use.
(defn reduce-+
  [rdr]
  ;;There is a fast path specifically for summations
  (dtype-reductions/commutative-binary-reduce
   :+ rdr))


(defn sum [rdr] (reduce-+ rdr))


(defn reduce-*
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   rdr (:* binary-op/builtin-ops)))


(defn reduce-max
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   rdr (:max binary-op/builtin-ops)))


(defn reduce-min
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   rdr (:min binary-op/builtin-ops)))


(defn mean
  ^double [rdr]
  (pmath// (double (reduce-+ rdr))
           (double (dtype-base/ecount rdr))))


(defn magnitude-squared
  [item]
  (-> (sq item)
      (reduce-+)))


(defn magnitude
  ^double [item]
  (Math/sqrt (double (magnitude-squared item))))


(defn dot-product
  [lhs rhs]
  (-> (* lhs rhs)
      (reduce-+)))

(defn distance-squared
  [lhs rhs]
  (magnitude-squared (- lhs rhs)))

(defn distance
  [lhs rhs]
  (magnitude (- lhs rhs)))


(defn equals
  [lhs rhs & [error-bar]]
  (clojure.core/< (double (distance lhs rhs))
                  (double (clojure.core/or error-bar 0.001))))


(export-symbols tech.v3.datatype.statistics
                descriptive-statistics
                variance
                standard-deviation
                median
                skew
                kurtosis
                quartile-1
                quartile-3
                pearsons-correlation
                spearmans-correlation
                kendalls-correlation
                percentiles
                quartiles
                quartile-outlier-fn)


(export-symbols tech.v3.datatype.rolling
                fixed-rolling-window)
