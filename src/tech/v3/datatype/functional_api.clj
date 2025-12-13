(ns tech.v3.datatype.functional-api
  "Arithmetic and statistical operations based on the Buffer interface.  These
  operators and functions all implement vectorized interfaces so passing in something
  convertible to a reader will return a reader.  Arithmetic operations are done lazily.
  These functions generally incur a large dispatch cost so for example each call to '+'
  checks all the arguments to decide if it should dispatch to an iterable implementation
  or to a reader implementation.  For tight loops or operations like map and filter,
  using the specific operators will result in far faster code than using the '+'
  function itself."
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.op-dispatch :refer [dispatch-unary-op
                                                  dispatch-binary-op] :as op-dispatch]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.export-symbols :refer [export-symbols] :as export-symbols]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1
                                               vectorized-dispatch-2]
             :as dispatch]
            [tech.v3.datatype.emap :as emap]
            ;;optimized operations
            [tech.v3.datatype.functional.opt :as fn-opt]
            [tech.v3.datatype.rolling]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.graal-native :as graal-native]
            [ham-fisted.api :as hamf]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.reduce :as hamf-rf]
            [clj-commons.primitive-math :as pmath]
            [clojure.tools.logging :as log]
            [clojure.set :as set])
  (:import [tech.v3.datatype BinaryOperator UnaryOperator Buffer
            LongReader DoubleReader ObjectReader ArrayHelpers]
           [org.roaringbitmap RoaringBitmap]
           [ham_fisted IMutList Casts]
           [java.util List]
           [java.util.function DoublePredicate DoubleConsumer]
           [clojure.lang IDeref]
           [org.apache.commons.math3.stat.regression SimpleRegression])
  (:refer-clojure :exclude [+ - / *
                            <= < >= >
                            identity
                            min max
                            bit-xor bit-and bit-and-not bit-not bit-set bit-test
                            bit-or bit-flip bit-clear
                            bit-shift-left bit-shift-right unsigned-bit-shift-right
                            quot rem cast not and or neg? even? zero? odd? pos?
                            finite? abs infinite? long double]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defn- all-arithmetic-ops
  [ns-sym]
  (->> (ns-publics ns-sym)
       (map (fn [[op-name op-var]]
              (let [op (deref op-var)]
                (when (clojure.core/or (instance? UnaryOperator op)
                                       (instance? BinaryOperator op))                  
                  [op-name op]))))
       (filter clojure.core/identity)
       (into {})))


(defmacro ^:private implement-arithmetic-operations
  []
  (let [binary-ops (all-arithmetic-ops 'tech.v3.datatype.binary-op)
        unary-ops (all-arithmetic-ops 'tech.v3.datatype.unary-op)
        binop-names (set (keys binary-ops))
        unop-names (set (keys unary-ops))
        dual-ops (set/intersection binop-names unop-names)]
    `(do
       ~@(->>
          (set/difference unop-names binop-names)
          (map
           (fn [opname]
             (let [op-sym (symbol "tech.v3.datatype.unary-op" (name opname))
                   op (unary-ops opname)
                   input-dtype (tech.v3.datatype.protocols/input-datatype op)
                   output-dtype (ham-fisted.protocols/returned-datatype op)]
               `(defn ~(with-meta opname
                         {:unary-operator op-sym})
                  ~@(if (clojure.core/and input-dtype output-dtype)
                      `[([~'x ~'options]
                         (dispatch-unary-op ~op-sym ~'x))
                        ([~'x]
                         (dispatch-unary-op ~op-sym ~'x))]
                      `[([~'x ~'options]
                         (dispatch-unary-op ~op-sym ~input-dtype ~output-dtype ~'x))
                        ([~'x]
                         (dispatch-unary-op ~op-sym ~input-dtype ~output-dtype ~'x))]))))))
       ~@(->>
          binary-ops
          (map
           (fn [[opname op]]
             (let [op-sym (symbol "tech.v3.datatype.binary-op" (name opname))
                   input-dtype (tech.v3.datatype.protocols/input-datatype op)
                   output-dtype (ham-fisted.protocols/returned-datatype op)
                   bin-dispatch
                   (if-not (clojure.core/and input-dtype output-dtype)
                     `[([~'x ~'y]
                        (dispatch-binary-op ~op-sym ~'x ~'y))
                       ([~'x ~'y & ~'args]
                        (reduce #(dispatch-binary-op ~op-sym %1 %2)
                                (lznc/concat [~'x ~'y] ~'args)))]
                     `[([~'x ~'y]
                        (dispatch-binary-op ~op-sym ~input-dtype ~output-dtype ~'x ~'y))
                       ([~'x ~'y & ~'args]
                        (reduce #(dispatch-binary-op ~op-sym ~input-dtype ~output-dtype %1 %2)
                                (lznc/concat [~'x ~'y] ~'args)))])]
               (if (dual-ops opname)
                 (let [un-op-sym (symbol "tech.v3.datatype.unary-op" (name opname))]
                   `(defn ~(with-meta opname
                             {:binary-operator op-sym
                              :unary-operator un-op-sym})
                      ([~'x]
                       (dispatch-unary-op ~un-op-sym ~'x nil))
                      ~@bin-dispatch))
                 `(defn ~(with-meta opname
                           {:binary-operator opname})
                    ~@bin-dispatch)))))))))


(implement-arithmetic-operations)


(defn- round-scalar
  ^long [^double x]
  (Math/round x))


(defn round
  "Vectorized implementation of Math/round.  Operates in double space
  but returns a long or long reader."
  ([x options]
   (vectorized-dispatch-1
    round-scalar
    (fn [_dtype x] (dispatch/typed-map-1 round-scalar :int64 x))
    (fn [_op-dtype ^Buffer src-rdr]
      (reify LongReader
        (lsize [rdr] (.lsize src-rdr))
        (readLong [rdr idx]
          (Math/round (.readDouble src-rdr idx)))))
    (merge {:operation-space :float64} options)
    x))
  ([x]
   (round x nil)))

;;Implement only reductions that we know we will use.
(defn reduce-+
  "Invokes a reduce operation with the operand + to add items together.

  Examples:

  ```clojure
  user> (dfn/reduce-+ [1 2 3 4])
  10
  user> (dfn/reduce-+ [-1 1.1 4])
  4.1
  ```"
  [x]
  ;;There is a fast path specifically for summations
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/+ binary-op/builtin-ops) x))


(defn reduce-*
  "Invokes a reduce operation with the operand * to multiply items together.

  Examples:

  ```clojure
  user> (dfn/reduce-* [1 2 3 4])
  24
  user> (dfn/reduce-* [-1 1.1 4])
  -4.4
  ```"
  [x]
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/* binary-op/builtin-ops) x))


(defn reduce-max
  "Invokes a reduce operation with the operand max find the maximum item of a list.

  Examples:

  ```clojure
  user> (dfn/reduce-max [1 2 3 4])
  4
  user> (dfn/reduce-max [10 -10 20 100 -1])
  100
  ```"
  [x]
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/max binary-op/builtin-ops) x))


(defn reduce-min
  "Invokes a reduce operation with the operand min find the minimum item of a list.

  Examples:

  ```clojure
  user> (dfn/reduce-min [1 2 3 4])
  1
  user> (dfn/reduce-min [10 -10 20 100 -1])
  -10
  ```"
  [x]
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/min binary-op/builtin-ops) x))


(defn- vm-major-version
  ^long []
  (let [vs (System/getProperty "java.version")
        periodIdx (.indexOf vs ".")]
    (try (Integer/parseInt (if (== periodIdx -1)
                             vs
                             (.substring vs 0 periodIdx)))
         (catch Exception e 0))))


(def ^:private optimized-opts*
  (delay
    {:sum fn-opt/sum
     :dot-product fn-opt/dot-product
     :magnitude-squared fn-opt/magnitude-squared
     :distance-squared fn-opt/distance-squared}
    ;;vec ops disabled as distance-squared requires nan-checking when used with
    ;;columns with missing values.
    #_(graal-native/when-not-defined-graal-native
     (when (clojure.core/>= (vm-major-version) 17)
       (try
         ((requiring-resolve 'tech.v3.datatype.functional.vecopt/optimized-operations))
         (catch Exception e
           (log/debug "JDK17 vector ops unavailable - to enable please enable vector op module:
\t--add-modules jdk.incubator.vector")
           {}))))))




(defn sum-fast
  "Find the sum of the data.  This operation is neither nan-aware nor does it implement
  kahans compensation although via parallelization it implements pairwise summation
  compensation.  For a more but slightly slower but far more correct sum operator,
  use [[sum]]."
  ^double [x]
  (hamf/sum-fast (clojure.core/or (dtype-base/as-reader x :float64) x)))


(defn mean-fast
  "Take the mean of the x.  This operation doesn't know anything about nan hence it is
  a bit faster than the base [[mean]] fn."
  ^double [x]
  (/ (sum-fast x) (dtype-base/ecount x)))


(defn magnitude-squared
  ^double [x]
  (double ((:magnitude-squared @optimized-opts*) x)))


(defn dot-product
  ^double [x y]
  (double ((:dot-product @optimized-opts*) x y)))


(defn distance-squared
  ^double [x y]
  (double ((:distance-squared @optimized-opts*) x y)))


(defn magnitude
  ^double [x]
  (Math/sqrt (magnitude-squared x)))


(defn normalize
  [x]
  (let [mag (magnitude x)]
    (-> (/ x mag)
        (vary-meta assoc :magnitude mag))))


(defn distance
  ^double [x y]
  (Math/sqrt (distance-squared x y)))


(defn equals
  [x y & [error-bar]]
  (clojure.core/< (Casts/doubleCast (distance x y))
                  (Casts/doubleCast (clojure.core/or error-bar 0.001))))


(defmacro ^:private implement-unary-predicates
  []
  `(do
     ~@(->> unary-pred/builtin-ops
            (map (fn [[k _v]]
                   (let [fn-symbol (symbol (name k))]
                     `(let [v# (unary-pred/builtin-ops ~k)]
                        (defn ~(with-meta fn-symbol
                                 {:unary-predicate k})
                          ([~'x ~'options]
                           (vectorized-dispatch-1
                            v#
                            (fn [dtype# x#] (unary-pred/iterable v# x#))
                            (fn [dtype# x#] (unary-pred/reader v# x#))
                            (merge (meta v#) ~'options)
                            ~'x))
                          ([~'x]
                           (~fn-symbol ~'x nil))))))))))


(implement-unary-predicates)


(defmacro ^:private implement-binary-predicates
  []
  `(do
     ~@(->> [:tech.numerics/and
             :tech.numerics/or
             :tech.numerics/eq
             :tech.numerics/bit-test
             :tech.numerics/not-eq]
            (map (fn [k]
                   (let [fn-symbol (symbol (name k))]
                     `(let [v# (binary-pred/builtin-ops ~k)]
                        (defn ~(with-meta fn-symbol
                                 {:binary-predicate k})
                          ([~'x ~'y]
                           (vectorized-dispatch-2
                            v#
                            (fn [op-dtype# lhs# rhs#]
                              (binary-pred/iterable v# lhs# rhs#))
                            (fn [op-dtype# lhs-rdr# rhs-rdr#]
                              (binary-pred/reader v# lhs-rdr# rhs-rdr#))
                            (meta v#)
                            ~'x ~'y))))))))))


(implement-binary-predicates)


(defmacro ^:private implement-compare-predicates
  []
  `(do
     ~@(->> [:tech.numerics/>
             :tech.numerics/>=
             :tech.numerics/<
             :tech.numerics/<=]
            (map (fn [k]
                   (let [fn-symbol (symbol (name k))]
                     `(let [v# (binary-pred/builtin-ops ~k)]
                        (defn ~(with-meta fn-symbol
                                 {:binary-predicate k})
                          ([~'x ~'y ~'z]
                           (and
                            (~fn-symbol ~'x ~'y)
                            (~fn-symbol ~'y ~'z)))
                          ([~'x ~'y]
                           (vectorized-dispatch-2
                            v#
                            (fn [op-dtype# lhs# rhs#]
                              (binary-pred/iterable v# lhs# rhs#))
                            (fn [op-dtype# lhs-rdr# rhs-rdr#]
                              (binary-pred/reader v# lhs-rdr# rhs-rdr#))
                            (meta v#)
                            ~'x ~'y))))))))))


(implement-compare-predicates)


(export-symbols tech.v3.datatype.unary-pred
                bool-reader->indexes)


(export-symbols tech.v3.datatype.statistics
                descriptive-statistics
                variance
                standard-deviation
                mode
                mean
                sum
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
  [x max-span]
  (let [num-reader (dtype-base/->reader x :float64)
        max-span (Casts/doubleCast max-span)
        n-elems (.lsize num-reader)
        n-spans (dec n-elems)
        retval
        (parallel-for/indexed-map-reduce
         n-spans
         (fn [^long start-idx ^long group-len]
           (let [new-data (dtype-list/make-list :float64)
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
                         divisor (Math/ceil span-fract)
                         add-data (pmath// span-len divisor)]
                     (dotimes [add-idx (long num-new-data)]
                       (.addDouble new-data (pmath/+ lhs
                                                     (pmath/* add-data
                                                              (unchecked-inc
                                                               (Casts/doubleCast add-idx)))))
                       (.add new-indexes (pmath/+ cur-new-idx add-idx)))))))
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


(deftype CumOpConsumer [^BinaryOperator op
                        ^{:unsynchronized-mutable true
                         :tag 'double} sum
                        ^:unsynchronized-mutable first
                        ^DoublePredicate p
                        ^IMutList result]
  DoubleConsumer
  (accept [this v]
    (if (.test p v)
      (do
        (if first
          (do
            (set! sum v)
            (set! first false))
          (set! sum (.binaryDouble op sum v)))
        (.addDouble result sum))
      (.addDouble result Double/NaN)))
  IDeref
  (deref [this] result))


(defn- ^:no-doc cumop
  [options ^BinaryOperator op data]
  (let [data (if (dtype-base/reader? data)
               (dtype-base/as-reader data :float64)
               data)
        n-elems (if (instance? Buffer data)
                  (.lsize ^Buffer data)
                  8)
        result (hamf/double-array-list n-elems)
        ^DoublePredicate filter
        (case (get options :nan-strategy :remove)
          :keep (hamf-fn/double-predicate v true)
          :remove (hamf-fn/double-predicate v (not (Double/isNaN v)))
          :exception (hamf-fn/double-predicate v
                                            (do
                                              (when (Double/isNaN v)
                                                (throw (RuntimeException. "Nan Detected")))
                                              true)))]
    @(reduce hamf-rf/double-consumer-accumulator
             (CumOpConsumer. op 0.0 true filter result)
             data)))


(defn cumsum
  "Cumulative running summation; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([x options]
   (cumop options (binary-op/builtin-ops :tech.numerics/+) x))
  ([x]
   (cumsum x nil)))


(defn cummin
  "Cumulative running min; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([x options]
   (cumop options (binary-op/builtin-ops :tech.numerics/min) x))
  ([x]
   (cummin x nil)))


(defn cummax
  "Cumulative running max; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([x options]
   (cumop options (binary-op/builtin-ops :tech.numerics/max) x))
  ([x]
   (cummax x nil)))


(defn cumprod
  "Cumulative running product; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([x options]
   (cumop options (binary-op/builtin-ops :tech.numerics/*) x))
  ([x]
   (cumprod x nil)))


(defn linear-regressor
  "Create a simple linear regressor.  Returns a function that given a (double) 'x'
  predicts a (double) 'y'.  The function has metadata that contains the regressor and
  some regressor info, notably slope and intercept.

  Example:

```clojure
tech.v3.datatype.functional> (def regressor (linear-regressor [1 2 3] [4 5 6]))
#'tech.v3.datatype.functional/regressor
tech.v3.datatype.functional> (regressor 1)
4.0
tech.v3.datatype.functional> (regressor 2)
5.0
tech.v3.datatype.functional> (meta regressor)
{:regressor
  #object[org.apache.commons.math3.stat.regression.SimpleRegression 0x52091e82 \"org.apache.commons.math3.stat.regression.SimpleRegression@52091e82\"],
 :intercept 3.0,
 :slope 1.0,
 :mean-squared-error 0.0}
```"
  [x y]
  (let [reg (SimpleRegression.)
        x (dtype-base/->reader x :float64)
        y (dtype-base/->reader y :float64)]
    (errors/when-not-errorf
     (== (.lsize x) (.lsize y))
     "x length (%d) doesn't match y length (%d)"
     (.lsize x) (.lsize y))
    (dotimes [idx (.size x)]
      (.addData reg (.readDouble x idx) (.readDouble y idx)))
    (with-meta
      #(.predict reg (Casts/doubleCast %))
      {:regressor reg
       :intercept (.getIntercept reg)
       :slope (.getSlope reg)
       :mean-squared-error (.getMeanSquareError reg)})))


(defn shift
  "Shift by n and fill in with the first element for n>0 or last element for n<0.

  Examples:

```clojure
user> (dfn/shift (range 10) 2)
[0 0 0 1 2 3 4 5 6 7]
user> (dfn/shift (range 10) -2)
[2 3 4 5 6 7 8 9 9 9]
```"
  ^Buffer [x n]
  (let [rdr (dtype-base/->reader x)
        dtype (casting/simple-operation-space (.elemwiseDatatype rdr))
        n-elems (.lsize rdr)
        max-idx (dec n-elems)
        n (long n)]
    (case dtype
      :int64
      (reify LongReader
        (elemwiseDatatype [r] (.elemwiseDatatype rdr))
        (lsize [r] (.lsize rdr))
        (readLong [r idx] (.readLong rdr (max 0 (min max-idx (- idx n))))))
      :float64
      (reify DoubleReader
        (elemwiseDatatype [r] (.elemwiseDatatype rdr))
        (lsize [r] (.lsize rdr))
        (readDouble [r idx] (.readDouble rdr (max 0 (min max-idx (- idx n))))))
      (reify ObjectReader
        (elemwiseDatatype [r] (.elemwiseDatatype rdr))
        (lsize [r] (.lsize rdr))
        (readObject [r idx] (.readObject rdr (max 0 (min max-idx (- idx n)))))))))


(comment
  (export-symbols/write-api! 'tech.v3.datatype.functional-api
                             'tech.v3.datatype.functional
                             "src/tech/v3/datatype/functional.clj"
                             '[+ - / *
                               <= < >= >
                               identity
                               min max
                               bit-xor bit-and bit-and-not bit-not bit-set bit-test
                               bit-or bit-flip bit-clear
                               bit-shift-left bit-shift-right unsigned-bit-shift-right
                               quot rem cast not and or
                               neg? even? zero? odd? pos?])
  )
