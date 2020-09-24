(ns tech.v3.datatype.functional
  "Arithmetic and statistical operations based on the PrimitiveIO interface.  These
  operators and functions all implement vectorized interfaces so passing in something
  convertible to a reader will return a reader.  Arithmetic operations are done lazily.
  These functions generally incur a large dispatch cost so for example each call to '+'
  checks all the arguments to decide if it should dispatch to an iterable implementation
  or to a reader implementation.  For tight loops or operations like map and filter,
  using the specific operators will result in far faster code than using the '+'
  function itself."
  (:require [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.const-reader :refer [const-reader]]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1
                                               vectorized-dispatch-2]
             :as dispatch]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.rolling]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.list :as dtype-list]
            [primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype BinaryOperator PrimitiveReader
            LongReader DoubleReader ObjectReader
            UnaryOperator BinaryOperator
            UnaryPredicate BinaryPredicate
            PrimitiveList]
           [org.roaringbitmap RoaringBitmap]
           [java.util List])
  (:refer-clojure :exclude [+ - / *
                            <= < >= >
                            identity
                            min max
                            bit-xor bit-and bit-and-not bit-not bit-set bit-test
                            bit-or bit-flip bit-clear
                            bit-shift-left bit-shift-right unsigned-bit-shift-right
                            quot rem cast not and or
                            neg? even? zero? odd? pos?]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ->unary-operator
  "Convert a thing to a unary operator. Thing can be a keyword or
  an implementation of IFn or an implementation of a UnaryOperator."
  ^UnaryOperator [op]
  (clojure.core/or
   (get unary-op/builtin-ops op)
   (unary-op/->operator op)))


(defn ->binary-operator
  "Convert a thing to a binary operator.  Thing can be a keyword or
  an implementation of IFn or an implementation of a BinaryOperator."
  ^BinaryOperator [op]
  (clojure.core/or
   (get binary-op/builtin-ops op)
   (binary-op/->operator op)))


(defn ->unary-predicate
  "Convert a thing to a unary predicate. Thing can be a keyword or
  an implementation of IFn or an implementation of a UnaryPredicate."
  ^UnaryPredicate [op]
  (clojure.core/or
   (get unary-pred/builtin-ops op)
   (unary-pred/->predicate op)))


(defn binary-predicate
  "Convert a thing to a binary predicate.  Thing can be a keyword or
  an implementation of IFn or an implementation of a BinaryPredicate."
  ^BinaryPredicate [op]
  (clojure.core/or
   (get binary-pred/builtin-ops op)
   (binary-pred/->predicate op)))


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
                   (unary-op/builtin-ops ~opname)
                   ;;the default iterable application is fine.
                   nil
                   #(unary-op/reader
                     (unary-op/builtin-ops ~opname)
                     %1
                     %2)
                   ~op-meta
                   ~'x))))))
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
                      (unary-op/builtin-ops ~opname)
                      nil
                      #(unary-op/reader
                        (unary-op/builtin-ops ~opname)
                        %1
                        %2)
                      ~op-meta
                      ~'x))
                    ([~'x ~'y]
                     (vectorized-dispatch-2
                      (binary-op/builtin-ops ~opname)
                      #(binary-op/iterable (binary-op/builtin-ops ~opname)
                                           %1 %2 %3)
                      #(binary-op/reader
                        (binary-op/builtin-ops ~opname)
                        %1 %2 %3)
                      ~op-meta
                      ~'x ~'y))
                    ([~'x ~'y & ~'args]
                     (reduce ~op-sym (concat [~'x ~'y] ~'args))))
                 `(defn ~(with-meta op-sym
                           {:binary-operator opname})
                    ([~'x ~'y]
                     (vectorized-dispatch-2
                      (binary-op/builtin-ops ~opname)
                      #(binary-op/iterable (binary-op/builtin-ops ~opname)
                                           %1 %2 %3)
                      #(binary-op/reader
                        (binary-op/builtin-ops ~opname)
                        %1 %2 %3)
                      ~op-meta
                      ~'x ~'y))
                    ([~'x ~'y & ~'args]
                     (reduce ~op-sym (concat [~'x ~'y] ~'args))))))))))))


(implement-arithmetic-operations)


(defn- round-scalar
  ^long [^double arg]
  (Math/round arg))

(defn round
  "Vectorized implementation of Math/round.  Operates in double space
  but returns a long or long reader."
  [arg]
  (vectorized-dispatch-1
   round-scalar
   (fn [dtype arg] (dispatch/typed-map-1 round-scalar :int64 arg))
   (fn [op-dtype rdr]
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
   (:+ binary-op/builtin-ops) rdr))


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
                           nil
                           ~'lhs ~'rhs)))))))))


(implement-binary-predicates)


(export-symbols tech.v3.datatype.unary-pred
                unary-pred/bool-reader->indexes)


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


(defn fill-range
  "Given a reader of numeric data and a max span amount, produce
  a new reader where the difference between any two consecutive elements
  is less than or equal to the max span amount.  Also return a bitmap of the added
  indexes.  Uses linear interpolation to fill in areas, operates in double space.
  Returns
  {:result :missing}"
  [numeric-data max-span]
  (let [num-reader (dtype-base/->reader numeric-data)
        max-span (double max-span)
        n-elems (.lsize num-reader)
        n-spans (dec n-elems)
        dec-max-span (dec max-span)
        retval
        (parallel-for/indexed-map-reduce
         n-spans
         (fn [^long start-idx ^long group-len]
           (let [^PrimitiveList new-data (dtype-list/make-list :float64)
                 new-indexes (RoaringBitmap.)]
             (dotimes [idx group-len]
               (let [idx (pmath/+ idx start-idx)
                     lhs (.readDouble num-reader idx)
                     rhs (.readDouble num-reader (unchecked-inc idx))
                     span-len (pmath/- rhs lhs)
                     _ (.addDouble new-data lhs)
                     cur-new-idx (.size new-data)]
                 (when (pmath/>= span-len max-span)
                   (let [span-fract (pmath// span-len max-span)
                         num-new-data (Math/floor span-fract)
                         num-new-data (if (== num-new-data span-fract)
                                        (unchecked-dec num-new-data)
                                        num-new-data)
                         divisor (Math/ceil span-fract)]
                     (let [add-data (pmath// span-len divisor)]
                       (dotimes [add-idx (long num-new-data)]
                         (.addDouble new-data (pmath/+ lhs
                                                       (pmath/* add-data
                                                                (unchecked-inc
                                                                 (double add-idx)))))
                         (.add new-indexes (pmath/+ cur-new-idx add-idx))))))))
             {:result new-data
              :missing new-indexes}))
         (partial reduce
                  (fn [{:keys [^List result missing]} new-data]
                    (let [res-size (.size result)]
                      (.addAll result ^List (:result new-data))
                      (.or ^RoaringBitmap missing
                           (RoaringBitmap/addOffset
                            ^RoaringBitmap (:missing new-data)
                            res-size)))
                    {:result result
                     :missing missing})))
        ^List result (:result retval)]
    (.add result (.readDouble num-reader (unchecked-dec (.lsize num-reader))))
    (assoc retval :result result)))
