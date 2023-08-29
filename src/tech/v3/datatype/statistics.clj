(ns tech.v3.datatype.statistics
  "Nan-aware, high quality and reasonably efficient summation and descriptive statistics."
  (:require [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.array-buffer :as abuf]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.list]
            [clj-commons.primitive-math :as pmath]
            [ham-fisted.api :as hamf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.mut-map :as hamf-map]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.set :as set])
  (:import [tech.v3.datatype UnaryOperator
            Buffer
            DoubleConsumers$MinMaxSum
            DoubleConsumers$Moments
            UnaryPredicates$DoubleUnaryPredicate]
           [org.apache.commons.math3.stat.descriptive.rank Percentile
            Percentile$EstimationType]
           [org.apache.commons.math3.stat.ranking NaNStrategy]
           [org.apache.commons.math3.stat.correlation
            KendallsCorrelation PearsonsCorrelation SpearmansCorrelation]
           [java.util Arrays])
    (:refer-clojure :exclude [min max]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def ^:private stats-tower
  {:sum {:reduction (constantly :tech.numerics/+)}
   :min {:reduction (constantly (:min binary-op/builtin-ops))}
   :max {:reduction (constantly (:max binary-op/builtin-ops))}
   :mean {:dependencies [:sum]
          :formula (fn [stats-data]
                     (pmath// (double (:sum stats-data))
                              (double (:n-elems stats-data))))}
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
                                  (unchecked-dec (double (:n-elems stats-data)))))}
   :standard-deviation {:dependencies [:variance]
                        :formula (fn [stats-data]
                                   (Math/sqrt (double (:variance stats-data))))}
   :skew {:dependencies [:moment-3 :standard-deviation]
          :formula (fn [stats-data]
                     (let [n-elemsd (double (:n-elems stats-data))
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
                         (let [n-elemsd (double (:n-elems stats-data))]
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
               (let [^UnaryOperator op (reduction stat-data)
                     {:keys [n-elems sum]}
                     (->> (dtype-base/as-reader rdr :float64)
                          (lznc/map (hamf-fn/double-unary-operator
                                     v (.unaryDouble op v)))
                          (hamf/sum-stable-nelems options))]
                 (assoc stat-data statname sum :n-elems n-elems))
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


(def all-descriptive-stats-names
  #{:min :quartile-1 :sum :mean :mode :median :quartile-3 :max
    :variance :standard-deviation :skew :n-elems :kurtosis})

(hamf-rf/bind-double-consumer-reducer! #(DoubleConsumers$MinMaxSum.))

(defn- key?
  [e] (when e (key e)))


(defn mode
  "Return the value of the most common occurance in the data."
  [data]
  (hamf/mode (or (dtype-base/as-reader data) data)))


(defn descriptive-statistics
  "Calculate a set of descriptive statistics on a single reader.

  Available stats:
  #{:min :quartile-1 :sum :mean :mode :median :quartile-3 :max
    :variance :standard-deviation :skew :n-elems :kurtosis}

  options
    - `:nan-strategy` - defaults to :remove, one of
    [:keep :remove :exception]. The fastest option is :keep but this
    may result in your results having NaN's in them.  You can also pass
  in a double predicate to filter custom double values."
  ([x stats-names stats-data {:keys [nan-strategy]
                              :or {nan-strategy :remove}
                              :as options}]
   (let [stats-set (hamf/immut-set stats-names)
         stats-data (or stats-data {})
         median? (stats-set :median)
         mode? (stats-set :mode)
         percentile-set #{:quartile-1 :quartile-3}
         percentile? (some stats-set percentile-set)
         percentile-set (set/intersection stats-set percentile-set)
         stats-set (disj (set/difference stats-set percentile-set) :mode)
         rdr (->> (or (dtype-base/as-reader x :float64) x)
                  (hamf/apply-nan-strategy options))
         ;;update options to reflect filtering
         options (assoc options :nan-strategy :keep)
         rdr (->> (if (or median? percentile?)
                    (let [darray (dtype-cmc/->array-buffer :float64 rdr)]
                      ;;arrays/sort is blindingly fast.
                      (when median?
                        (Arrays/sort ^doubles (.ary-data darray)
                                     (.offset darray)
                                     (+ (.offset darray)
                                        (.n-elems darray))))
                      darray)
                    rdr))
         stats-data (merge
                     (cond
                       (or (hamf/empty? x)
                           (try (hamf/empty? rdr) (catch Throwable e true)))
                       {:n-elems 0
                        :min ##NaN
                        :max ##NaN
                        :median ##NaN
                        :mode nil}
                       (and (or median? percentile?) (not (hamf/empty? rdr)))
                       (let [n-elems (dtype-base/ecount rdr)]
                         (if median?
                           {:min (rdr 0)
                            :max (rdr -1)
                            :median (rdr (quot n-elems 2))
                            :n-elems n-elems}
                           {:min (rdr 0)
                            :max (rdr -1)
                            :n-elems n-elems})))
                     stats-data)
         stats-data (if mode?
                      (assoc stats-data :mode (mode x))
                      stats-data)
         provided-keys (hamf-map/keyset stats-data)
         calculate-stats-set (set/difference stats-set provided-keys)
         dependency-set (apply set/reduce-union (map node-dependencies calculate-stats-set))
         required-dependency-set (set/difference dependency-set provided-keys)
         stats-data (if (not (empty? (set/intersection required-dependency-set
                                                       #{:sum :min :max :n-elems})))
                      (merge stats-data (hamf-rf/preduce-reducer (DoubleConsumers$MinMaxSum.)
                                                              rdr))
                      stats-data)
         stats-data (if (required-dependency-set :mean)
                      (assoc stats-data :mean (/ (double (stats-data :sum))
                                                 (double (stats-data :n-elems))))
                      stats-data)
         stats-data (if (not (empty? (set/intersection required-dependency-set
                                                       #{:moment-2 :moment-3 :moment-4})))
                      (merge stats-data (hamf-rf/preduce-reducer
                                         (hamf-rf/double-consumer-reducer
                                          #(DoubleConsumers$Moments. (:mean stats-data)))
                                         rdr))
                      stats-data)
         provided-keys (hamf-map/keyset stats-data)
         required-dependency-set (set/difference required-dependency-set provided-keys)
         stats-data (reduce #(calculate-descriptive-stat %2 %1 options rdr)
                            stats-data
                            ;;any leftover stats-tower items must be formula-driven based on
                            ;;precalculated stats.
                            (set/intersection (hamf-map/keyset stats-tower)
                                              required-dependency-set))
         stats-data (if percentile?
                      (let [p (Percentile.)
                            ary-buf (abuf/as-array-buffer rdr)]
                        (.setData p ^doubles (.ary-data ary-buf) (.offset ary-buf)
                                  (.n-elems ary-buf))
                        (merge stats-data
                               (when (:quartile-1 percentile-set)
                                 {:quartile-1 (.evaluate p 25.0)})
                               (when (:quartile-3 percentile-set)
                                 {:quartile-3 (.evaluate p 75.0)})))
                      stats-data)]
     (select-keys stats-data (-> (set/union stats-set percentile-set)
                                 (set/union (when mode? #{:mode}))))))
  ([x stats-names options]
   (descriptive-statistics x stats-names nil options))
  ([x stats-names]
   (descriptive-statistics x stats-names nil nil))
  ([x]
   (descriptive-statistics x [:n-elems :min :mean :max :standard-deviation]
                           nil nil)))


(defmacro define-descriptive-stats
  []
  `(do
     ~@(->> (dissoc stats-tower :mean :min :max :sum)
            (map (fn [[tower-key tower-node]]
                   (let [fn-symbol (symbol (name tower-key))]
                     (if (:dependencies tower-node)
                       `(defn ~fn-symbol
                          (~(with-meta ['x 'options] {:tag 'double})
                           (~tower-key (descriptive-statistics ~'x #{~tower-key}
                                                               ~'options)))
                          (~(with-meta ['x]
                              {:tag 'double})
                           (~fn-symbol ~'x nil)))
                       `(defn ~fn-symbol
                          (~(with-meta ['x 'options] {:tag 'double})
                           (~tower-key (calculate-descriptive-stat
                                        ~tower-key nil ~'options ~'x)))
                          (~(with-meta ['x] {:tag 'double})
                           (~fn-symbol ~'x nil))))))))))


(define-descriptive-stats)

(defn- dreader [data] (or (dtype-base/as-reader data :float64) data))


(defn sum
  "Double sum of data using
  [Kahan compensated summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm)."
  (^double [x options]
   (dtype-reductions/double-summation options x))
  (^double [x]
   (sum x nil)))


(defn min
  (^double [x options]
   (let [x (dreader x)]
     (if (hamf/empty? x)
       Double/NaN
       (dtype-reductions/commutative-binary-double
        (:tech.numerics/min binary-op/builtin-ops) options x))))
  (^double [x]
   (min x nil)))


(defn max
  (^double [x options]
   (let [x (dreader x)]
     (if (hamf/empty? x)
       Double/NaN
       (dtype-reductions/commutative-binary-double
        (:tech.numerics/max binary-op/builtin-ops) options x))))
  (^double [x]
   (max x nil)))


(defn mean
  "double mean of x"
  (^double [x options]
   (let [x (dreader x)]
     (if (hamf/empty? x)
       Double/NaN
       (let [{:keys [n-elems sum]} (dtype-reductions/staged-double-consumer-reduction
                                    :tech.numerics/+ options x)]
         (pmath// (double sum)
                  (double n-elems))))))
  (^double [x]
   (mean x nil)))


(defn median
  (^double [x options]
   (:median (descriptive-statistics x [:median] options)))
  (^double [x]
   (:median (descriptive-statistics x [:median]))))


(defn quartile-1
  (^double[x options]
   (:quartile-1 (descriptive-statistics x [:quartile-1] options)))
  (^double [x]
   (:quartile-1 (descriptive-statistics x [:quartile-1]))))


(defn quartile-3
  (^double [x options]
   (:quartile-3 (descriptive-statistics x [:quartile-3] options)))
  (^double [x]
   (:quartile-3 (descriptive-statistics x [:quartile-3]))))


(defn pearsons-correlation
  (^double [x y options]
   (-> (PearsonsCorrelation.)
       (.correlation (dtype-cmc/->double-array options x)
                     (dtype-cmc/->double-array options y))))
  (^double [x y]
   (pearsons-correlation x y nil)))


(defn spearmans-correlation
  (^double [x y options]
   (-> (SpearmansCorrelation.)
       (.correlation (dtype-cmc/->double-array options x)
                     (dtype-cmc/->double-array options y))))
  (^double [x y]
   (spearmans-correlation x y nil)))


(defn kendalls-correlation
  (^double [x y options]
   (-> (KendallsCorrelation.)
       (.correlation (dtype-cmc/->double-array options x)
                     (dtype-cmc/->double-array options y))))
  (^double [x y]
   (kendalls-correlation x y nil)))


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
  (^Buffer [x percentages options]
   (let [ary-buf (dtype-cmc/->array-buffer :float64 options x)
         p (doto (.withEstimationType
                  (Percentile.)
                  (options->percentile-estimation-strategy options))
             (.setData ^doubles (.ary-data ary-buf) (.offset ary-buf)
                       (.n-elems ary-buf)))]
     (dtype-base/->reader (mapv #(.evaluate p (double %)) percentages))))
  (^Buffer [x percentages]
   (percentiles x percentages nil)))


(defn quartiles
  "return [min, 25 50 75 max] of item"
  (^Buffer [x]
   (percentiles x [0.001 25 50 75 100]))
  (^Buffer [x options]
   (percentiles x [0.001 25 50 75 100] options)))


(defn quartile-outlier-fn
  "Create a function that, given floating point data, will return true or false
  if that data is an outlier.  Default range mult is 1.5:

```clojure
  (or (< val (- q1 (* range-mult iqr)))
      (> val (+ q3 (* range-mult iqr)))
```

  Options:
  * `:range-mult` - the multiplier used."
  [x & {:keys [range-mult]}]
  (let [[q1 q3] (percentiles x [25 75])
        q1 (double q1)
        q3 (double q3)
        iqr (- q3 q1)
        range-mult (double (or range-mult 1.5))
        lowb (- q1 (* range-mult iqr))
        highb (+ q3 (* range-mult iqr))]
    (hamf-fn/double-predicate
     x (or (< x lowb)
           (> x highb)))))
