(ns tech.v3.datatype.main
  (:require [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype :as dtype])
  (:import [tech.v3.datatype PrimitiveReader]
           [xerial.larray.mmap MMapBuffer MMapMode]
           [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn -main
  [& args]
  (let [n-elems 100000
        darray (dtype/make-container :float64 (range n-elems))
        sum (dfn/reduce-+ darray)]
    (println (format "sum finished-%s" sum))
    (println "loop timings")
    (time (dotimes [iter 1000]
            (dfn/reduce-+ darray)))
    0))
