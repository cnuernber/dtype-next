(ns tech.v3.datatype.statistics
  (:require [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype DoubleReduction UnaryOperator]
           [org.apache.commons.math3.stat.descriptive.rank Percentile]
           [org.apache.commons.math3.stat.ranking NaNStrategy]
           [java.util Arrays Map])
    (:refer-clojure :exclude [min max]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def ^:private sum-double-reduction
  (reify UnaryOperator
    (unaryDouble [this arg] arg)))


(def ^:private min-double-reduction
  (reify DoubleReduction
    (update [this accum value]
      (pmath/min accum value))))


(def ^:private max-double-reduction
  (reify DoubleReduction
    (update [this accum value]
      (pmath/max accum value))))


(def stats-tower
  {:sum {:reduction (constantly sum-double-reduction)}
   :min {:reduction (constantly min-double-reduction)}
   :max {:reduction (constantly max-double-reduction)}
   :mean {:dependencies [:sum]
          :formula (fn [stats-data ^double n-elems]
                     (pmath// (double (:sum stats-data))
                              n-elems))}
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
              :formula (fn [stats-data ^double n-elems]
                         (pmath// (double (:moment-2 stats-data))
                                  (unchecked-dec n-elems)))}
   :standard-deviation {:dependencies [:variance]
                        :formula (fn [stats-data ^double n-elems]
                                   (Math/sqrt (double (:variance stats-data))))}
   :skew {:dependencies [:moment-3 :standard-deviation]
          :formula (fn [stats-data ^double n-elemsd]
                     (let [n-elems-12 (pmath/* (pmath/- n-elemsd 1.0)
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
              :formula (fn [stats-data ^double n-elemsd]
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
                           Double/NaN))}})


(def node-dependencies
  (memoize
   (fn [node-kwd]
     (let [node (node-kwd stats-tower)]
       (->>
        (concat [node-kwd]
                (mapcat node-dependencies (:dependencies node)))
        (set))))))


(def reduction-rank
  (memoize
   (fn [item]
     (let [node (stats-tower item)
           node-deps (:dependencies node)
           node-rank (long (if (:reduction node)
                             1
                             0))]
       (+ node-rank
          (long (apply clojure.core/max 0 (map reduction-rank node-deps))))))))


(def reduction-groups
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


(defn calculate-descriptive-stat
  ([statname stat-data rdr]
   (if (stat-data statname)
     stat-data
     (if-let [{:keys [dependencies reduction formula]} (get stats-tower statname)]
       (let [stat-data (reduce #(calculate-descriptive-stat %2 %1 rdr)
                               stat-data
                               dependencies)
             stat-data (if reduction
                         (merge stat-data
                                (dtype-reductions/double-reductions
                                 {statname (reduction stat-data)}
                                 rdr))
                         stat-data)
             stat-data (if formula
                         (assoc stat-data statname
                                (formula stat-data
                                         (double
                                          (dtype-base/ecount rdr))))
                         stat-data)]
         stat-data)
       (throw (Exception. (format "Unrecognized descriptive statistic: %s"
                                  statname))))))
  ([statname rdr]
   (calculate-descriptive-stat statname {} rdr)))


(def all-descriptive-stats-names
  [:min :percentile-1 :sum :mean :mode :median :percentile-3 :max
   :variance :standard-deviation :skew :n-values :kurtosis])


(defn- ->double-array
  ^doubles [item]
  (if-let [ary-data (dtype-base/->array-buffer item)]
    (if (and (= :float64 (dtype-base/elemwise-datatype ary-data))
             (== 0 (.offset ary-data))
             (== (dtype-base/ecount (.ary-data ary-data))
                 (.n-elems ary-data)))
      (.ary-data ary-data)
      (-> (dtype-cmc/make-container :float64 item)
          (->double-array)))
    (-> (dtype-cmc/make-container :float64 item)
        (->double-array))))


(defn descriptive-statistics
  ([stats-names rdr stats-data]
   (if (== 0 (dtype-base/ecount rdr))
     (->> stats-names
          (map (fn [sname]
                 [sname (if (= sname :n-values)
                          0
                          Double/NaN)]))
          (into {})))
   (let [rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)
         stats-set (set stats-names)
         median? (and (stats-set :median)
                      (not (contains? stats-data :median)))
         ^PrimitiveIO rdr (if median?
                            (let [darray (->double-array rdr)]
                              ;;arrays/sort is blindingly fast.
                              (Arrays/sort darray)
                              (dtype-base/->reader darray))
                            rdr)
         stats-data (merge (if median?
                             {:min (rdr 0)
                              :max (rdr (unchecked-dec n-elems))
                              :median (rdr (quot n-elems 2))
                              :n-values n-elems}
                             {:n-values n-elems})
                           stats-data)
         calculate-stats-set (set/difference stats-set (set (keys stats-data)))
         dependency-set (reduce set/union (map node-dependencies calculate-stats-set))
         calculated-dependency-set (reduce set/union
                                           (map node-dependencies (keys stats-data)))
         required-dependency-set (set/difference dependency-set
                                                 calculated-dependency-set)
         stats-data (reduce
                     (fn [stats-data group]
                       (let [reductions (:reductions group)
                             dependencies (:dependencies group)
                             ;;these caclculates are guaranteed to not need
                             ;;to do any reductions.
                             stats-data (reduce #(calculate-descriptive-stat
                                                  %2
                                                  %1
                                                  rdr)
                                                stats-data
                                                dependencies)]
                         (merge
                          (dtype-reductions/double-reductions
                           (->> reductions
                                (map (fn [[kwd red-fn]]
                                       [kwd (red-fn stats-data)]))
                                (into {}))
                           rdr)
                          stats-data)))
                     stats-data
                     (reduction-groups required-dependency-set))
         stats-data (reduce #(calculate-descriptive-stat %2 %1 rdr)
                            stats-data
                            calculate-stats-set)]
     (select-keys stats-data stats-set)))
  ([stats-names rdr]
   (descriptive-statistics stats-names rdr {}))
  ([rdr]
   (descriptive-statistics [:n-values :min :mean :max :standard-deviation] rdr {})))


(defmacro define-descriptive-stats
  []
  `(do
     ~@(->> stats-tower
            (map (fn [[tower-key tower-node]]
                   (let [fn-symbol (symbol (name tower-key))]
                     (if (:dependencies tower-node)
                       `(defn ~fn-symbol
                          [~'data]
                          (~tower-key (descriptive-statistics #{~tower-key} ~'data)))
                       `(defn ~fn-symbol
                          [~'data]
                          (~tower-key (calculate-descriptive-stat
                                       ~tower-key ~'data))))))))))


(define-descriptive-stats)


(defn median
  [data]
  (:median (descriptive-statistics [:median] data)))


(comment

  (import '[org.apache.commons.math3.stat.descriptive DescriptiveStatistics])


  )
