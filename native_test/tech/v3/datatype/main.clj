(ns tech.v3.datatype.main
  (:require [tech.v3.datatype.array-buffer :as ary-buf]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.copy-make-container :as copy-cmc])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn -main
  [& args]
  (let [data (copy-cmc/make-container :jvm-heap :float32 (range 10))]
    (println data))
  0)
