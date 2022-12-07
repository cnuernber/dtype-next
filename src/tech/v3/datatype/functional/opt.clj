(ns tech.v3.datatype.functional.opt
  "Optimized tech.v3.datatype.functional operations.  These do not account for NaN
  values in any way and thus cannot be used in data where NaN may be present or that
  has any missing values."
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.parallel.for :as pfor]
            [com.github.ztellman.primitive-math :as pmath]
            [tech.v3.datatype.errors :as errors])
  (:import [tech.v3.datatype Buffer]))

(defn ->reader
  ^Buffer [data]
  (or (dt-base/as-reader data :float64)
      (dt-base/->reader (vec data))))

(defn sum
  ^double [data]
  (let [data (->reader data)
        n-elems (.lsize data)]
    (pfor/indexed-map-reduce
     n-elems
     (fn [sidx glen]
       (let [sidx (long sidx)
             glen (long glen)
             eidx (+ sidx glen)]
         (loop [idx sidx
                sum 0.0]
           (if (< idx eidx)
             (recur (unchecked-inc idx) (pmath/+ sum (.readDouble data idx)))
             sum))))
     (partial reduce +))))


(defn ensure-equal-len
  [lhs rhs]
  (errors/when-not-errorf
   (== (dt-base/ecount lhs)
       (dt-base/ecount rhs))
   "lhs length (%d) is not equal to rhs length (%d)"
   (dt-base/ecount lhs) (dt-base/ecount rhs)))


(defn dot-product
  ^double [lhs rhs]
  (let [lhs (->reader lhs)
        rhs (->reader rhs)]
    (ensure-equal-len lhs rhs)
    (pfor/indexed-map-reduce
     (.lsize lhs)
     (fn [start-idx glen]
       (let [start-idx (long start-idx)
             glen (long glen)
             eidx (pmath/+ start-idx glen)]
         (loop [idx start-idx
                sum 0.0]
           (if (< idx eidx)
             (recur (unchecked-inc idx)
                    (pmath/+ sum (pmath/* (.readDouble lhs idx)
                                          (.readDouble rhs idx))))
             sum))))
     (partial reduce +))))


(defn magnitude-squared
  ^double [data]
  (let [data (->reader data)
        n-elems (.lsize data)]
    (pfor/indexed-map-reduce
     n-elems
     (fn [sidx glen]
       (let [sidx (long sidx)
             glen (long glen)
             eidx (+ sidx glen)]
         (loop [idx sidx
                sum 0.0]
           (if (< idx eidx)
             (let [temp (.readDouble data idx)]
               (recur (unchecked-inc idx) (pmath/+ sum (pmath/* temp temp))))
             sum))))
     (partial reduce +))))


(defn distance-squared
  [lhs rhs]
  (let [lhs (->reader lhs)
        rhs (->reader rhs)]
    (ensure-equal-len lhs rhs)
    (pfor/indexed-map-reduce
     (.lsize lhs)
     (fn [start-idx glen]
       (let [start-idx (long start-idx)
             glen (long glen)
             eidx (pmath/+ start-idx glen)]
         (loop [idx start-idx
                sum 0.0]
           (if (< idx eidx)
             (let [ll (.readDouble lhs idx)
                   rr (.readDouble rhs idx)
                   temp (if (and (Double/isNaN ll) (Double/isNaN rr))
                          0.0
                          (pmath/- ll rr))]
               (recur (unchecked-inc idx)
                      (pmath/+ sum (pmath/* temp temp))))
             sum))))
     (partial reduce +))))
