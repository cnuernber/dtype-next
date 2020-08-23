(ns tech.v3.datatype.main
  (:require [tech.v3.datatype.copy-make-container :refer [make-container]]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype PrimitiveReader]
           [xerial.larray.mmap MMapBuffer MMapMode]
           [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe])
  (:gen-class))

(defn -main
  [& args]
  (let [n-elems 10000000
        darray (dtype-base/->reader (make-container :float64 (range n-elems)))
        sum (parallel-for/indexed-map-reduce
             n-elems
             (fn [^long start-idx ^long group-len]
               (let [end-idx (+ start-idx group-len)]
                 (loop [idx start-idx
                        sum 0.0]
                   (if (< idx end-idx)
                     (recur (unchecked-inc idx) (pmath/+ sum (.readDouble darray idx)))
                     sum))))
             (partial reduce +))]
    (println (format "sum finished-%s" sum))
    0))
