(ns tech.v3.datatype.functional
  "Arithmetic and statistical operations based on the Buffer interface.  These
  operators and functions all implement vectorized interfaces so passing in something
  convertible to a reader will return a reader.  Arithmetic operations are done lazily.
  These functions generally incur a large dispatch cost so for example each call to '+'
  checks all the arguments to decide if it should dispatch to an iterable implementation
  or to a reader implementation.  For tight loops or operations like map and filter,
  using the specific operators will result in far faster code than using the '+'
  function itself."
  (:require [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.const-reader :refer [const-reader]]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1
                                               vectorized-dispatch-2]
             :as dispatch]
            ;;optimized operations
            [tech.v3.datatype.functional.opt :as fn-opt]
            [tech.v3.datatype.rolling]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.graal-native :as graal-native]
            [primitive-math :as pmath]
            [clojure.tools.logging :as log]
            [clojure.set :as set])
  (:import [tech.v3.datatype BinaryOperator Buffer
            LongReader DoubleReader ObjectReader
            UnaryOperator BinaryOperator
            UnaryPredicate BinaryPredicate
            PrimitiveList ArrayHelpers]
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
                  ([~'x ~'options]
                   (vectorized-dispatch-1
                    (unary-op/builtin-ops ~opname)
                    ;;the default iterable application is fine.
                    nil
                    #(unary-op/reader
                      (unary-op/builtin-ops ~opname)
                      %1
                      %2)
                    (merge ~op-meta ~'options)
                    ~'x))
                  ([~'x]
                   (~op-sym ~'x  nil)))))))
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
  ([arg options]
   (vectorized-dispatch-1
    round-scalar
    (fn [dtype arg] (dispatch/typed-map-1 round-scalar :int64 arg))
    (fn [op-dtype ^Buffer src-rdr]
      (reify LongReader
        (lsize [rdr] (.lsize src-rdr))
        (readLong [rdr idx]
          (Math/round (.readDouble src-rdr idx)))))
    (merge {:operation-space :float64} options)
    arg))
  ([arg]
   (round arg nil)))

;;Implement only reductions that we know we will use.
(defn reduce-+
  [rdr]
  ;;There is a fast path specifically for summations
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/+ binary-op/builtin-ops) rdr))


(defn reduce-*
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/* binary-op/builtin-ops) rdr))


(defn reduce-max
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/max binary-op/builtin-ops) rdr))


(defn reduce-min
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   (:tech.numerics/min binary-op/builtin-ops) rdr))


(defonce ^:private optimized-opts* (atom {:sum fn-opt/sum
                                          :dot-product fn-opt/dot-product
                                          :magnitude-squared fn-opt/magnitude-squared
                                          :distance-squared fn-opt/distance-squared}))



(defn ^:no-doc register-optimized-operations!
  "Register one more more optimized operations.  Only specific operations are optimized
  via vectorization - sum dot-product magnitude-squared and distance-squared"
  [opt-map]
  (swap! optimized-opts* merge opt-map)
  (keys opt-map))


(graal-native/when-not-defined-graal-native
 (try
   (register-optimized-operations!
    ((requiring-resolve 'tech.v3.datatype.functional.vecopt/optimized-operations)))
   (catch Throwable e
     (log/debugf "JDK16 vector ops are not available: %s" (.getMessage e)))))


(defn sum
  "Find the sum of the data.  This operation is neither nan-aware nor does it implement
  kahans compensation although via parallelization it implements pairwise summation
  compensation.  For nan-aware and extremely correct summations please see the
  [[tech.v3.datatype.statistics]] namespace."
  ^double [data]
  (double ((:sum @optimized-opts*) data)))


(defn mean
  "Take the mean of the data.  This operation doesn't know anything about nan - for nan-aware
  operations use the statistics namespace."
  ^double [data]
  (/ (sum data) (dtype-base/ecount data)))


(defn magnitude-squared
  ^double [data]
  (double ((:magnitude-squared @optimized-opts*) data)))


(defn dot-product
  ^double [lhs rhs]
  (double ((:dot-product @optimized-opts*) lhs rhs)))


(defn distance-squared
  ^double [lhs rhs]
  (double ((:distance-squared @optimized-opts*) lhs rhs)))


(defn magnitude
  (^double [item options]
   (-> (magnitude-squared item)
       (Math/sqrt)))
  (^double [item]
   (magnitude item nil)))


(defn distance
  ^double [lhs rhs]
  (Math/sqrt (distance-squared lhs rhs)))


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
                          ([~'arg ~'options]
                           (vectorized-dispatch-1
                            v#
                            (fn [dtype# item#] (unary-pred/iterable v# item#))
                            (fn [dtype# item#] (unary-pred/reader v# item#))
                            (merge (meta v#) ~'options)
                            ~'arg))
                          ([~'arg]
                           (~fn-symbol ~'arg nil))))))))))


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
                          ([~'lhs ~'rhs ~'options]
                           (vectorized-dispatch-2
                            v#
                            (fn [op-dtype# lhs# rhs#]
                              (binary-pred/iterable v# lhs# rhs#))
                            (fn [op-dtype# lhs-rdr# rhs-rdr#]
                              (binary-pred/reader v# lhs-rdr# rhs-rdr#))
                            (merge (meta v#) ~'options)
                            ~'lhs ~'rhs))
                          ([~'lhs ~'rhs]
                           (~fn-symbol ~'lhs ~'rhs nil))))))))))


(implement-binary-predicates)


(export-symbols tech.v3.datatype.unary-pred
                unary-pred/bool-reader->indexes)


(export-symbols tech.v3.datatype.statistics
                descriptive-statistics
                variance
                standard-deviation
                median
                skew
                mean
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
  (let [num-reader (dtype-base/->reader numeric-data :float64)
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


(defn ^:no-doc cumop
  [options ^BinaryOperator op data]
  (let [^Buffer data (if (dtype-base/reader? data)
                       (dtype-base/as-reader data :float64)
                       (dtype-base/as-reader (vec data) :float64))
        n-elems (.lsize data)
        result (double-array n-elems)
        filter (dtype-reductions/nan-strategy->double-predicate
                (:nan-strategy options :remove))]
    (when-not (pmath/== 0 n-elems)
      (loop [idx 0
             any-valid? false
             sum (.readDouble data 0)]
        (when (pmath/< idx n-elems)
          (let [next-elem (.readDouble data idx)
                valid? (boolean (if filter (.test filter next-elem) true))
                sum (double (if valid?
                              (if any-valid?
                                (.binaryDouble op sum next-elem)
                                next-elem)
                              sum))]
            (if valid?
              (ArrayHelpers/aset result idx sum)
              (ArrayHelpers/aset result idx next-elem))
            (recur (unchecked-inc idx) (clojure.core/or any-valid? valid?) sum)))))
    (array-buffer/array-buffer result)))


(defn cumsum
  "Cumulative running summation; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([options data]
   (cumop options (binary-op/builtin-ops :tech.numerics/+) data))
  ([data]
   (cumsum nil data)))


(defn cummin
  "Cumulative running min; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([options data]
   (cumop options (binary-op/builtin-ops :tech.numerics/min) data))
  ([data]
   (cummin nil data)))


(defn cummax
  "Cumulative running max; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([options data]
   (cumop options (binary-op/builtin-ops :tech.numerics/max) data))
  ([data]
   (cummax nil data)))


(defn cumprod
  "Cumulative running product; returns result in double space.

  Options:

  * `:nan-strategy` - one of `:keep`, `:remove`, `:exception`.  Defaults to `:remove`."
  ([options data]
   (cumop options (binary-op/builtin-ops :tech.numerics/*) data))
  ([data]
   (cumprod nil data)))
