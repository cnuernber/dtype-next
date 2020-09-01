(ns tech.v3.datatype.statistics
  (:require [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype DoubleReduction]
           [java.util Arrays]))


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
  {:sum {:reduction sum-double-reduction}
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
                                   (Math/sqrt (double (:variance stats-data))))}})


(defn ^{:dependencies #{:mean}
        :reduction } variance
  ^double ([rdr moments]
           (let [moments (ensure-stat-dependencies
                          #{:mean}
                          moments rdr)])))


(defn ^{:dependencies #{:standard-deviation :moment-3}}
  skew
  (^double [rdr moments]
   (let [moments (ensure-stat-dependencies
                  #{:standard-deviation :moment-3}
                  moments rdr)
         moment-3 (double (:moment-3 moments))
         stddev (double (:standard-deviation moments))
         n-elems-12 (pmath/* (pmath/- n-elemsd 1.0)
                             (pmath/- n-elemsd 2.0))]
     (if (>= n-elemsd 3.0)
       (pmath// (pmath/* (pmath// n-elemsd n-elems-12)
                         moment-3)
                (pmath/* stddev (pmath/* stddev stddev)))
       Double/NaN)))
  (^double [rdr]
   (skew [rdr moments])))


(defn moment-stats
  [^double mean stats-set rdr]
  (let [mean (double mean)
        n-elemsd (double (dtype-base/ecount rdr))
        mean-reductions
        (dtype-reductions/double-reductions
         {:variance
           reduction {:moment-3
                               (fn [^double mean]
                                 (let [mean (double mean)]
                                   (reify DoubleReduction
                                     (initialize [this value]
                                       (let [item (pmath/- value mean)]
                                         (pmath/* (pmath/* item item) item)))
                                     (update [this accum value]
                                       (pmath/+ accum (.initialize this value)))
                                     (merge [this lhs rhs] (pmath/+ lhs rhs)))))}
          :moment-4 (reify DoubleReduction
                      (initialize [this value]
                        (let [item (pmath/- value mean)
                              item-sq (pmath/* item item)]
                          (pmath/* item-sq item-sq)))
                      (update [this accum value]
                        (pmath/+ accum (.initialize this value)))
                      (merge [this lhs rhs] (pmath/+ lhs rhs)))}
         rdr)
        variance (double (:variance mean-reductions))
        moment-3 (double (:moment-3 mean-reductions))
        moment-4 (double (:moment-4 mean-reductions))
        stddev (Math/sqrt variance)
        q-amt (pmath/* 0.67448975 stddev)
        n-elems-1 (pmath/- n-elemsd 1.0)
        n-elems-12 (pmath/* n-elems-1 (pmath/- n-elemsd 2.0))
        skew
        n-elems-123 (pmath/* n-elems-12 (pmath/- n-elemsd 3.0))
        n-elems-23 (pmath// n-elems-123
                            n-elems-1)
        kurt-n-num (pmath/* n-elemsd (pmath/* (pmath/+ n-elemsd 1.0)))
        kurt-prefix (pmath// kurt-n-num n-elems-123)
        kurt-lhs (pmath// (pmath/* kurt-prefix moment-4)
                          (pmath/* variance variance))
        kurt-rhs (pmath// (pmath/* 3.0 (pmath/* n-elems-1 n-elems-1))
                          n-elems-23)
        kurtosis (if (>= n-elemsd 4.0)
                   (pmath/- kurt-lhs kur-rhs)
                   Double/NaN)]
    ;;cheap percentile estimation
    {:variance variance
     :standard-deviation stddev
     :skew skew
     :kurtosis kurtosis
     :moment-3 moment-3
     :percentile-1 (pmath/- mean q-amt)
     :percentile-3 (pmath/+ mean q-amt)}))



(def all-descriptive-stats-names
  [:min :percentile-1 :sum :mean :mode :median :percentile-3 :max
   :variance :standard-deviation :skew :n-values])


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
         requires-stddev? (some stats-set [:standard-deviation :percentile-1
                                           :percentile-3 :skew :variance
                                           :kurtosis])
         requires-sum? (or requires-stddev? (some stats-set [:sum :mean]))
         requires-sort? (stats-set :median)
         ^PrimitiveIO rdr (if requires-sort?
                            (let [darray (->double-array rdr)]
                              (Arrays/sort darray)
                              (dtype-base/->reader rdr))
                            rdr)
         initial-reduction
         (merge (when requires-sort?
                  {:min (rdr 0)
                   :max (rdr (unchecked-dec n-elems))
                   :median (rdr (quot n-elems 2))})
                (when (or requires-sum?
                          (and (not requires-sort?)
                               (some stats-set [:min :max])))
                  (dtype-reductions/double-reductions
                   (merge {}
                          (when requires-sum?
                            {:sum sum-double-reduction})
                          (when (and (not requires-sort?) (stats-set :min))
                            {:min min-double-reduction})
                          (when (and (not requires-sort?) (stats-set :max))
                            {:max max-double-reduction}))
                   rdr)))
         sum-reductions
         (when requires-sum?
           (let [sum (double (:sum initial-reduction))
                 mean (pmath// sum (double n-elems))]
             (merge {:mean mean}
                    (when requires-stddev?
                      (moment-stats mean stats-set rdr)))))]
     (->> stats-names
          (map (juxt identity (merge {:n-values n-elems}
                                     initial-reduction
                                     sum-reductions)))
          (into {}))))
  ([rdr]
   (descriptive-statistics [:n-values :min :mean :max :standard-deviation])))
