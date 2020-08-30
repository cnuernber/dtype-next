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
  (reify DoubleReduction
    (update [this accum value]
      (pmath/+ accum value))))


(def ^:private min-double-reduction
  (reify DoubleReduction
    (update [this accum value]
      (pmath/min accum value))))


(def ^:private max-double-reduction
  (reify DoubleReduction
    (update [this accum value]
      (pmath/max accum value))))


(defn moment-stats
  [^double mean stats-set rdr]
  (let [mean (double mean)
        n-elemsd (double (dtype-base/ecount rdr))
        mean-reductions
        (dtype-reductions/double-reductions
         {:variance (reify DoubleReduction
                      (initialize [this value]
                        (let [item (pmath/- value mean)]
                          (pmath/* item item)))
                      (update [this accum value]
                        (pmath/+ accum (.initialize this value)))
                      (merge [this lhs rhs] (pmath/+ lhs rhs))
                      (finalize [this accum n-elems]
                        (pmath// accum (unchecked-dec n-elemsd))))
          :moment-3 (reify DoubleReduction
                      (initialize [this value]
                        (let [item (pmath/- value mean)]
                          (pmath/* (pmath/* item item) item)))
                      (update [this accum value]
                        (pmath/+ accum (.initialize this value)))
                      (merge [this lhs rhs] (pmath/+ lhs rhs)))}
         rdr)
        variance (double (:variance mean-reductions))
        moment-3 (double (:moment-3 mean-reductions))
        stddev (Math/sqrt variance)
        q-amt (pmath/* 0.67448975 stddev)
        skew (if (>= n-elemsd 3.0)
               (pmath// (pmath/* (pmath// n-elemsd
                                          (* (- n-elemsd 1.0)
                                             (- n-elemsd 2.0)))
                                 moment-3)
                        (pmath/* stddev (pmath/* stddev stddev)))
               Double/NaN)]
    ;;cheap percentile estimation
    {:variance variance
     :standard-deviation stddev
     :skew skew
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
                                           :percentile-3 :skew :variance])
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
