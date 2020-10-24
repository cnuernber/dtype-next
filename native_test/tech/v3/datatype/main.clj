(ns tech.v3.datatype.main
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.tensor :as dtt])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn -main
  [& args]
  (let [data (dtt/->tensor (partition 3 (range 9)) :datatype :float32)
        ld (dtype-dt/local-date)]
    (println data ld))
  0)
