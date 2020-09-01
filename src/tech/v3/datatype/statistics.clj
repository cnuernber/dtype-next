(ns tech.v3.datatype.statistics
  (:require [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype DoubleReduction]
           [java.util Arrays Map]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(def ^:private sum-double-reduction
  (reify DoubleReduction))


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
                             (reify DoubleReduction
                               (elemwise [this value]
                                 (let [item (pmath/- value mean)]
                                   (pmath/* item item))))))}
   :moment-3 {:dependencies [:mean]
              :reduction (fn [stats-data]
                           (let [mean (double (:mean stats-data))]
                             (reify DoubleReduction
                               (elemwise [this value]
                                 (let [item (pmath/- value mean)]
                                   (pmath/* item (pmath/* item item)))))))}
   :moment-4 {:dependencies [:mean]
              :reduction (fn [stats-data]
                           (let [mean (double (:mean stats-data))]
                             (reify DoubleReduction
                               (elemwise [this value]
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

(def tower-dependencies
  (->> stats-tower
       (map (fn [[k v]]
              (when-let [v-deps (:dependencies v)]
                [k v-deps])))
       (remove nil?)
       (into {})))


;;How many reductions does it take to get the answer
(def tower-reduction-dependencies
  (->> stats-tower
       (map (fn [[k v]]
              [k (->> (tree-seq #(get tower-dependencies %)
                                #(get tower-dependencies %)
                                k)
                      (distinct)
                      (filter #(and (:reduction (get stats-tower %))
                                    (not= k %)))
                      seq)]))
       (into {})))

(defn reduction-rank
  [item]
  (map
   (fn [red] (count (tree-seq #(get tower-reduction-dependencies %)
                              #(get tower-reduction-dependencies %)
                              red)))
   (tower-reduction-dependencies item)))

(def tower-reduction-ranks
  (->> (vals tower-reduction-dependencies)
       (apply concat)
       (distinct)
       (fn [red]
         (if (nil? (tower-reduction-dependencies red))
           0
           (+ 1 (apply max (map tower-reduction-dependencies ))))
         (if (= v #{k})
           1
           (apply max )))))


(defn reduction-groups
  [stats-seq]
  (let [all-reductions (->> stats-seq
                            (mapcat tower-reduction-dependencies)
                            distinct)]
    all-reductions))


(defn calculate-descriptive-stat
  [statname stat-data rdr]
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
      (throw (Exception. (format "Unrecognized statistic name: %s" statname))))))



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


(defn- rank-stats
  [stats-seq]
  (let [nodes (->> (map stats-tower stats-seq)
                   (remove nil?))]
    (when-not (== (count nodes) (count stats-seq))
      (throw (Exception. (format "Unrecognized statistics: %s"
                                 (set/difference (set stats-seq)
                                                 (set (keys stats-tower)))))))
    (->> nodes
         (map (fn [node]
                (assoc node :rank (count (tree-seq #(get-in stats-tower
                                                            [% :dependencies])
                                                   #(get-in stats-tower
                                                            [% :dependencies])
                                                   (:dependencies node))))))
         (group-by :rank)
         (sort-by first))))


(defn descriptive-statistics
  ([stats-names rdr]
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
         requires-sort? (stats-set :median)
         ^PrimitiveIO rdr (if requires-sort?
                            (let [darray (->double-array rdr)]
                              (Arrays/sort darray)
                              (dtype-base/->reader rdr))
                            rdr)
         stats-data (when requires-sort?
                      {:min (rdr 0)
                       :max (rdr (unchecked-dec n-elems))
                       :median (rdr (quot n-elems 2))})
         calculate-stats-set (if requires-sort?
                               (set/difference stats-set #{:min :max :median
                                                           :n-values})
                               (disj stats-set :n-values))
         ranked-stats (rank-stats calculate-stats-set)]

     (->> stats-names
          (map (juxt identity (merge {:n-values n-elems}
                                     initial-reduction
                                     sum-reductions)))
          (into {}))))
  ([rdr]
   (descriptive-statistics [:n-values :min :mean :max :standard-deviation])))

(comment

  (import '[org.apache.commons.math3.stat.descriptive DescriptiveStatistics])
  )
