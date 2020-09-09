(ns tech.v3.datatype.statistics
  (:require [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.list]
            [primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype DoubleReduction UnaryOperator PrimitiveIOIterator
            PrimitiveIO
            DoubleConsumers$MinMaxSum
            DoubleConsumers$Moments
            UnaryPredicate
            UnaryPredicates$DoubleUnaryPredicate]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [org.apache.commons.math3.stat.descriptive StorelessUnivariateStatistic]
           [org.apache.commons.math3.stat.descriptive.rank Percentile
            Percentile$EstimationType]
           [org.apache.commons.math3.stat.ranking NaNStrategy]
           [org.apache.commons.math3.stat.correlation
            KendallsCorrelation PearsonsCorrelation SpearmansCorrelation]
           [clojure.lang IFn]
           [java.util Arrays Map Iterator Spliterator Spliterator$OfDouble]
           [java.util.function DoubleConsumer])
    (:refer-clojure :exclude [min max]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def ^:private sum-double-reduction :+)


(defn- univariate-stat->consumer
  ^DoubleConsumer [^StorelessUnivariateStatistic stat]
    (reify
      DoubleConsumer
      (accept [this data]
        (.increment stat data))
      IFn
      (invoke [this]
        {:n-elems (.getN stat)
         :value (.getResult stat)})))


(def ^:private stats-tower
  {:sum {:reduction (constantly sum-double-reduction)}
   :min {:reduction (constantly (:min binary-op/builtin-ops))}
   :max {:reduction (constantly (:max binary-op/builtin-ops))}
   :mean {:dependencies [:sum]
          :formula (fn [stats-data]
                     (pmath// (double (:sum stats-data))
                              (double (:n-values stats-data))))}
   :moment-2 {:dependencies [:mean]
              :reduction (fn [stats-data]
                           (let [mean (double (:mean stats-data))]
                             (reify UnaryOperator
                               (unaryDouble [this value]
                                 (let [item (pmath/- value mean)]
                                   (pmath/* item item))))))}
   :moment-3 {:dependencies [:mean]
              :reduction (fn [stats-data]
                           (let [mean (double (:mean stats-data))]
                             (reify UnaryOperator
                               (unaryDouble [this value]
                                 (let [item (pmath/- value mean)]
                                   (pmath/* item (pmath/* item item)))))))}
   :moment-4 {:dependencies [:mean]
              :reduction (fn [stats-data]
                           (let [mean (double (:mean stats-data))]
                             (reify UnaryOperator
                               (unaryDouble [this value]
                                 (let [item (pmath/- value mean)
                                       item-sq (pmath/* item item)]
                                   (pmath/* item-sq item-sq))))))}

   :variance {:dependencies [:moment-2]
              :formula (fn [stats-data]
                         (pmath// (double (:moment-2 stats-data))
                                  (unchecked-dec (double (:n-values stats-data)))))}
   :standard-deviation {:dependencies [:variance]
                        :formula (fn [stats-data]
                                   (Math/sqrt (double (:variance stats-data))))}
   :skew {:dependencies [:moment-3 :standard-deviation]
          :formula (fn [stats-data]
                     (let [n-elemsd (double (:n-values stats-data))
                           n-elems-12 (pmath/* (pmath/- n-elemsd 1.0)
                                               (pmath/- n-elemsd 2.0))
                           stddev (double (:standard-deviation stats-data))
                           moment-3 (double (:moment-3 stats-data))]
                       (if (>= n-elemsd 3.0)
                         (pmath// (pmath/* (pmath// n-elemsd n-elems-12)
                                           moment-3)
                                  (pmath/* stddev (pmath/* stddev stddev)))
                         Double/NaN)))}
   ;;{ [n(n+1) / (n -1)(n - 2)(n-3)] sum[(x_i - mean)^4] / std^4 } - [3(n-1)^2 / (n-2)(n-3)]
   :kurtosis {:dependencies [:moment-4 :variance]
              :formula (fn [stats-data]
                         (let [n-elemsd (double (:n-values stats-data))]
                           (if (>= n-elemsd 4.0)
                             (let [variance (double (:variance stats-data))
                                   moment-4 (double (:moment-4 stats-data))
                                   nm1 (pmath/- n-elemsd 1.0)
                                   nm2 (pmath/- n-elemsd 2.0)
                                   nm3 (pmath/- n-elemsd 3.0)
                                   np1 (pmath/+ n-elemsd 1.0)
                                   nm23 (pmath/* nm2 nm3)
                                   prefix (pmath// (pmath/* n-elemsd np1)
                                                   (pmath/* nm1 nm23))
                                   central (pmath// moment-4
                                                    (pmath/* variance variance))
                                   rhs (pmath// (pmath/* 3.0 (pmath/* nm1 nm1))
                                                nm23)]
                               (pmath/- (pmath/* prefix central) rhs))
                             Double/NaN)))}})


(def ^:private node-dependencies
  (memoize
   (fn [node-kwd]
     (let [node (node-kwd stats-tower)]
       (->>
        (concat [node-kwd]
                (mapcat node-dependencies (:dependencies node)))
        (set))))))


(def ^:private reduction-rank
  (memoize
   (fn [item]
     (let [node (stats-tower item)
           node-deps (:dependencies node)
           node-rank (long (if (:reduction node)
                             1
                             0))]
       (+ node-rank
          (long (apply clojure.core/max 0 (map reduction-rank node-deps))))))))


(def ^:private reduction-groups
  (memoize
   (fn [stat-dependencies]
     (->> stat-dependencies
          (filter #(get-in stats-tower [% :reduction]))
          (group-by reduction-rank)
          (sort-by first)
          (map (fn [[rank kwds]]
                 {:reductions (->> kwds
                                   (map (fn [kwd]
                                          [kwd (get-in stats-tower
                                                       [kwd :reduction])]))
                                   (into {}))
                  :dependencies
                  (->> kwds
                       (mapcat #(get-in stats-tower [% :dependencies]))
                       set)}))))))


(defn- calculate-descriptive-stat
  "Calculate a single statistic.  Utility method for calculate-descriptive-stats
  method below."
  ([statname stat-data options rdr]
   (if (get stat-data statname)
     stat-data
     (if-let [{:keys [dependencies reduction formula]} (get stats-tower statname)]
       (let [stat-data (reduce #(calculate-descriptive-stat %2 %1 options rdr)
                               stat-data
                               dependencies)
             stat-data
             (if reduction
               (let [{:keys [n-elems data] :as result}
                     (dtype-reductions/double-reductions
                      {statname (reduction stat-data)}
                      rdr
                      options)]
                 (assoc stat-data
                        statname (get data statname)
                        :n-values n-elems))
               stat-data)
             stat-data (if formula
                         (assoc stat-data statname
                                (formula stat-data))
                         stat-data)]
         stat-data)
       (throw (Exception. (format "Unrecognized descriptive statistic: %s"
                                  statname))))))
  ([statname rdr]
   (calculate-descriptive-stat statname {} nil rdr)))


(defn- reduce-group
  "There are optimized versions of reductions that combine various operations
  into the same run.  It would be very cool if the compiler could do this
  optimization but we are either a ways away and we cannot load dynamic classes in
  the graal native pathway."
  [stats-data reductions options rdr]
  (let [reductions-set (set (keys reductions))
        stats-data (merge stats-data
                          (when (some reductions-set [:min :max :sum])
                            (let [result
                                  (dtype-reductions/staged-double-consumer-reduction
                                   #(DoubleConsumers$MinMaxSum.)
                                   options rdr)]
                              (merge {:n-values (.nElems result)}
                                     (into {} (.value result)))))
                          (when (some reductions-set [:moment-2 :moment-3 :moment-4])
                            (let [result
                                  (dtype-reductions/staged-double-consumer-reduction
                                   #(DoubleConsumers$Moments.
                                     (double (:mean stats-data)))
                                   options rdr)]
                              (merge {:n-values (.nElems result)}
                                     (into {} (.value result))))))
        reductions-set (set/difference reductions-set
                                       #{:min :max :sum :moment-2
                                         :moment-3 :moment-4})]
    (if (seq reductions-set)
      (let [{:keys [n-elems data]}
            (dtype-reductions/double-reductions
             (->> (select-keys reductions reductions-set)
                  (map (fn [[kwd red-fn]]
                         [kwd (red-fn stats-data)]))
                  (into {}))
                        options
                        rdr)]
        (merge {:n-values n-elems }
               data
               stats-data))
      stats-data)))


(defn- options->apache-nan-strategy
  ^NaNStrategy [options]
  (case (:nan-strategy options)
    :keep NaNStrategy/FIXED
    :exception NaNStrategy/FAILED
    NaNStrategy/REMOVED))


(def all-descriptive-stats-names
  #{:min :quartile-1 :sum :mean :mode :median :quartile-3 :max
    :variance :standard-deviation :skew :n-values :kurtosis})


(defn descriptive-statistics
  "Calculate a set of descriptive statistics.

  options
    - `:nan-strategy` - defaults to :remove, one of
    [:keep :remove :exception]. The fastest option is :keep but this
    may result in your results having NaN's in them.  You can also pass
  in a double predicate "
  ([stats-names stats-data options rdr]
   (if (== 0 (dtype-base/ecount rdr))
     (->> stats-names
          (map (fn [sname]
                 [sname (if (= sname :n-values)
                          0
                          Double/NaN)]))
          (into {})))
   (let [rdr (dtype-base/->reader rdr)
         stats-set (set stats-names)
         median? (stats-set :median)
         percentile-set #{:quartile-1 :quartile-3}
         percentile? (some stats-set percentile-set)
         percentile-set (set/intersection stats-set percentile-set)
         stats-set (set/difference stats-set percentile-set)
         ^PrimitiveIO rdr (if (or median? percentile?)
                            (let [darray (dtype-cmc/->array-buffer
                                          :float64 options rdr)]
                              ;;arrays/sort is blindingly fast.
                              (when median?
                                (Arrays/sort ^doubles (.ary-data darray)
                                             (.offset darray)
                                             (+ (.offset darray)
                                                (.n-elems darray))))
                              (dtype-base/->reader darray))
                            rdr)
         ;;In this case we have already filtered out nans at the cost of copying the
         ;;entire array of data.
         options (if median?
                   (assoc options :nan-strategy :keep)
                   options)
         stats-data (merge (when median?
                             (let [n-elems (dtype-base/ecount rdr)]
                               {:min (rdr 0)
                                :max (rdr (unchecked-dec n-elems))
                                :median (rdr (quot n-elems 2))
                                :n-values n-elems}))
                           stats-data)
         calculate-stats-set (set/difference stats-set (set (keys stats-data)))
         dependency-set (reduce set/union (map node-dependencies calculate-stats-set))
         calculated-dependency-set (reduce set/union
                                           (map node-dependencies (keys stats-data)))
         required-dependency-set (set/difference dependency-set
                                                 calculated-dependency-set)
         stats-data
         (->> (reduction-groups required-dependency-set)
              (reduce
               (fn [stats-data group]
                 (let [reductions (:reductions group)
                       dependencies (:dependencies group)
                       ;;these caclculations are guaranteed to not need
                       ;;to do any reductions.
                       stats-data (reduce #(calculate-descriptive-stat
                                            %2
                                            %1
                                            options
                                            rdr)
                                          stats-data
                                          dependencies)]
                   (reduce-group stats-data reductions options rdr)))
               stats-data))
         stats-data (reduce #(calculate-descriptive-stat %2 %1 options rdr)
                            stats-data
                            calculate-stats-set)
         stats-data (if percentile?
                      (let [p (doto (Percentile.)
                                (.withNaNStrategy (options->apache-nan-strategy
                                                   options)))
                            ary-buf (dtype-base/->array-buffer rdr)]
                        (.setData p ^doubles (.ary-data ary-buf) (.offset ary-buf)
                                  (.n-elems ary-buf))
                        (merge stats-data
                               (when (:quartile-1 percentile-set)
                                 {:quartile-1 (.evaluate p 25.0)})
                               (when (:quartile-3 percentile-set)
                                 {:quartile-3 (.evaluate p 75.0)})))
                      stats-data)]
     (select-keys stats-data (set/union stats-set percentile-set))))
  ([stats-names options rdr]
   (descriptive-statistics stats-names nil options rdr))
  ([stats-names rdr]
   (descriptive-statistics stats-names nil nil rdr))
  ([rdr]
   (descriptive-statistics [:n-values :min :mean :max :standard-deviation]
                            nil nil rdr)))


(defmacro define-descriptive-stats
  []
  `(do
     ~@(->> stats-tower
            (map (fn [[tower-key tower-node]]
                   (let [fn-symbol (symbol (name tower-key))]
                     (if (:dependencies tower-node)
                       `(defn ~fn-symbol
                          (~(with-meta ['options 'data] {:tag 'double})
                           (~tower-key (descriptive-statistics #{~tower-key} ~'data
                                                               ~'options)))
                          (~(with-meta ['data]
                              {:tag 'double})
                           (~fn-symbol ~'data nil)))
                       `(defn ~fn-symbol
                          (~(with-meta ['options 'data] {:tag 'double})
                           (~tower-key (calculate-descriptive-stat
                                        ~tower-key nil ~'options ~'data)))
                          (~(with-meta ['data] {:tag 'double})
                           (~fn-symbol ~'data nil))))))))))


(define-descriptive-stats)

(defn median
  (^double [options data]
   (:median (descriptive-statistics [:median] options data)))
  (^double [data]
   (:median (descriptive-statistics [:median] data))))


(defn quartile-1
  (^double[options data]
   (:quartile-1 (descriptive-statistics [:quartile-1] options data)))
  (^double [data]
   (:quartile-1 (descriptive-statistics [:quartile-1] data))))


(defn quartile-3
  (^double [options data]
   (:quartile-3 (descriptive-statistics [:quartile-3] options data)))
  (^double [data]
   (:quartile-3 (descriptive-statistics [:quartile-3] data))))


(defn pearsons-correlation
  (^double [options lhs rhs]
   (-> (PearsonsCorrelation.)
       (.correlation (dtype-cmc/->double-array options lhs)
                     (dtype-cmc/->double-array options rhs))))
  (^double [lhs rhs]
   (pearsons-correlation nil lhs rhs)))


(defn spearmans-correlation
  (^double [options lhs rhs]
   (-> (SpearmansCorrelation.)
       (.correlation (dtype-cmc/->double-array options lhs)
                     (dtype-cmc/->double-array options rhs))))
  (^double [lhs rhs]
   (spearmans-correlation nil lhs rhs)))


(defn kendalls-correlation
  (^double [options lhs rhs]
   (-> (KendallsCorrelation.)
       (.correlation (dtype-cmc/->double-array options lhs)
                     (dtype-cmc/->double-array options rhs))))
  (^double [lhs rhs]
   (kendalls-correlation nil lhs rhs)))


(defn- options->percentile-estimation-strategy
  ""
  ^Percentile$EstimationType [{:keys [estimation-type]}]
  (case estimation-type
    :r1 Percentile$EstimationType/R_1
    :r2 Percentile$EstimationType/R_2
    :r3 Percentile$EstimationType/R_3
    :r4 Percentile$EstimationType/R_4
    :r5 Percentile$EstimationType/R_5
    :r6 Percentile$EstimationType/R_6
    :r7 Percentile$EstimationType/R_7
    :r8 Percentile$EstimationType/R_8
    :r9 Percentile$EstimationType/R_9
    Percentile$EstimationType/LEGACY))


(defn percentiles
  "Create a reader of percentile values, one for each percentage passed in.
  Estimation types are in the set of #{:r1,r2...legacy} and are described
  here: https://commons.apache.org/proper/commons-math/javadocs/api-3.3/index.html.

  nan-strategy can be one of [:keep :remove :exception] and defaults to :exception."
  (^PrimitiveIO [percentages options data]
   (let [ary-buf (dtype-cmc/->array-buffer :float64 {:nan-strategy :keep} data)
         p (doto (Percentile.)
             (.withNaNStrategy (options->apache-nan-strategy options))
             (.withEstimationType (options->percentile-estimation-strategy options))
             (.setData ^doubles (.ary-data ary-buf) (.offset ary-buf)
                       (.n-elems ary-buf)))]
     (dtype-base/->reader (mapv #(.evaluate p (double %)) percentages))))
  (^PrimitiveIO [percentages data]
   (percentiles percentages nil data)))


(defn quartiles
  "return [min, 25 50 75 max] of item"
  (^PrimitiveIO [item]
   (percentiles [0.001 25 50 75 100] item))
  (^PrimitiveIO [options item]
   (percentiles [0.001 25 50 75 100] options item)))


(defn quartile-outlier-fn
  "Create a function that, given floating point data, will return true or false
  if that data is an outlier.  Default range mult is 1.5:
  (or (< val (- q1 (* range-mult iqr)))
      (> val (+ q3 (* range-mult iqr)))"
  [item & [range-mult]]
  (let [[_ q1 _ q3 _] (quartiles item)
        q1 (double q1)
        q3 (double q3)
        iqr (- q3 q1)
        range-mult (double (or range-mult 1.5))]
    (reify UnaryPredicates$DoubleUnaryPredicate
      (unaryDouble [this x]
        (or (< x (- q1 (* range-mult iqr)))
            (> x (+ q3 (* range-mult iqr))))))))


(comment

  (do
    (import '[org.apache.commons.math3.stat.descriptive DescriptiveStatistics])
    (import '[tech.v3.datatype PrimitiveIODoubleSpliterator])
    (import '[java.util.stream StreamSupport])
    (import '[java.util.function DoubleBinaryOperator DoublePredicate])
    (require '[criterium.core :as crit])
    (def double-data (double-array (range 1000000)))
    (def print-consumer (reify java.util.function.DoubleConsumer
                          (accept [this val]
                            (println val)))))


  (defn benchmark-standard-stats-set
    []
    (crit/quick-bench
     (descriptive-statistics [:min :max :mean :standard-deviation :skew]
                             double-data)))


  (defn benchmark-descriptive-stats-set
    []
    (crit/quick-bench
     (let [desc-stats (DescriptiveStatistics. double-data)]
       {:min (.getMin desc-stats)
        :max (.getMax desc-stats)
        :mean (.getMean desc-stats)
        :standard-deviation (.getStandardDeviation desc-stats)
        :skew (.getSkewness desc-stats)})))

  (defn data->spliterator
    [data]
    (let [rdr (dtype-base/->reader data)
          spliterator (PrimitiveIODoubleSpliterator. rdr 0
                                                     (.lsize rdr)
                                                     :remove)]
      spliterator))

  (defn benchmark-math3-data
    []
    (let [consumer (moment-consumer)
          rdr (dtype-base/->reader double-data)]
      (dotimes [idx (.lsize rdr)]
        (.accept consumer (.readDouble rdr idx)))
      (consumer)))

  (defn spliterator-sum
    [data]
    (let [rdr (dtype-base/->reader data)
          spliterator (PrimitiveIODoubleSpliterator. rdr 0
                                                     (.lsize rdr)
                                                     :keep)
          stream (-> (StreamSupport/doubleStream spliterator true))]
      (.reduce stream 0.0 (reify DoubleBinaryOperator
                            (applyAsDouble [this lhs rhs]
                              (pmath/+ lhs rhs))))))

  (defn iterator-sum
    [data nan-strategy]
    (let [rdr (dtype-base/->reader data)]
      (parallel-for/indexed-map-reduce
       (dtype-base/ecount data)
       (fn [^long start-idx ^long group-len]
         (let [sub-buf (dtype-base/sub-buffer rdr start-idx group-len)
               iterable (->nan-aware-iterable sub-buf {:nan-strategy nan-strategy})
               ^PrimitiveIOIterator iterator (.iterator iterable)]
           (loop [continue? (.hasNext iterator)
                  accum 0.0]
             (if continue?
               (let [accum (pmath/+ accum (.nextDouble iterator))]
                 (recur (.hasNext iterator) accum))
               accum))))
       (partial reduce +))))


  )
