(ns tech.v3.datatype.functional.vecopt
  "Vectorized operations.  Requires JDK-16 with the vector incubator module added:
  :jvm-opts [\"--add-modules\" \"jdk.incubator.vector\"]"
  (:require [tech.v3.datatype.copy-make-container :as copy-cmc]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.array-buffer]
            [tech.v3.datatype.functional.opt :as fn-opt]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.parallel.for :as pfor])
  (:import [tech.v3.datatype VecOps]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [java.util.concurrent ForkJoinPool]))


(set! *warn-on-reflection* true)


(def ^{:tag ForkJoinPool
       :doc "Vectorized instructions don't benefit from using all available
threads on most systems.  Half of the available threads are generally sufficient to
flood the math units."} vecop-pool*
  (delay (ForkJoinPool. (quot (.availableProcessors (Runtime/getRuntime)) 2))))


(defn as-double-array-buffer
  ^ArrayBuffer [data]
  (when-let [ary-buf (dt-base/as-array-buffer data)]
    (when (identical? (.elemwise-datatype ary-buf) :float64)
      ary-buf)))


(defn parallelized-vecop-sum
  [n-elems op-fn]
  (pfor/indexed-map-reduce
   n-elems
   op-fn
   (partial reduce +)
   {:fork-join-pool @vecop-pool*}))


(defn sum
  ^double [data]
  (if-let [ary-buf (as-double-array-buffer data)]
    (parallelized-vecop-sum
     (.n-elems ary-buf)
     #(VecOps/sum (.ary-data ary-buf) (+ (.offset ary-buf) (int %1)) (int %2)))
    (fn-opt/sum data)))


(defn dot-product
  ^double [lhs rhs]
  (let [lhs-buf (as-double-array-buffer lhs)
        rhs-buf (as-double-array-buffer rhs)]
    (if (and lhs-buf rhs-buf)
      (do
        (fn-opt/ensure-equal-len lhs rhs)
        (parallelized-vecop-sum
         (.n-elems lhs-buf)
         #(VecOps/dot (.ary-data lhs-buf) (+ (.offset lhs-buf) (int %1))
                      (.ary-data rhs-buf) (+ (.offset rhs-buf) (int %1))
                      %2)))
      (fn-opt/dot-product lhs rhs))))


(defn magnitude-squared
  ^double [data]
  (if-let [ary-buf (as-double-array-buffer data)]
    (parallelized-vecop-sum
     (.n-elems ary-buf)
     #(VecOps/magnitudeSquared (.ary-data ary-buf) (+ (.offset ary-buf) (int %1)) %2))
    (fn-opt/magnitude-squared data)))


(defn distance-squared
  ^double [lhs rhs]
  (let [lhs-buf (as-double-array-buffer lhs)
        rhs-buf (as-double-array-buffer rhs)]
    (if (and lhs-buf rhs-buf)
      (do
        (fn-opt/ensure-equal-len lhs rhs)
        (parallelized-vecop-sum
         (.n-elems lhs-buf)
         #(VecOps/distanceSquared (.ary-data lhs-buf) (+ (.offset lhs-buf) (int %1))
                                  (.ary-data rhs-buf) (+ (.offset rhs-buf) (int %1))
                                  %2)))
      (fn-opt/distance-squared lhs rhs))))


(defn optimized-operations
  []
  {:sum sum
   :dot-product dot-product
   :magnitude-squared magnitude-squared
   :distance-squared distance-squared})
