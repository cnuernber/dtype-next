(ns tech.v3.datatype.main
  (:require [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype DoubleReader]
           [xerial.larray.mmap MMapBuffer MMapMode]
           [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe])
  (:gen-class))

(defn -main
  [& args]
  (let [n-elems 10000000
        ^DoubleReader darray (dtype-proto/->reader
                              (array-buffer/array-buffer
                               (double-array (range n-elems))) {})
        sum (parallel-for/indexed-map-reduce
             n-elems
             (fn [^long start-idx ^long group-len]
               (let [end-idx (+ start-idx group-len)]
                 (loop [idx start-idx
                        sum 0.0]
                   (if (< idx end-idx)
                     (recur (unchecked-inc idx) (pmath/+ sum (.read darray idx)))
                     sum))))
             (partial reduce +))]
    (println (format "sum finished-%s-attempting mmap" sum))
    (println "attempting to load the library")
    (System/load "/home/chrisn/dev/cnuernber/dtype-next/resources/liblarray.so")
    (MMapBuffer. (java.io.File. "project.clj") MMapMode/READ_ONLY)
    (println "mmap successful -- attempting unsafe access")
    (println "native buffer test - uses sun.misc.Unsafe")
    (println (vec (dtype-proto/->reader (native-buffer/malloc 10 {:resource-type :gc})
                                        {})))
    0))
