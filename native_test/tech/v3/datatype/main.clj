(ns tech.v3.datatype.main
  (:require [tech.v3.tensor :as dtt])
  (:import [tech.v3.datatype UnsafeUtil])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn -main
  [& args]
  (let [tens (dtt/->tensor (partition 3 (range 9)))]
    (println (tens 1 2))
    (println UnsafeUtil/addressFieldOffset))
  0)
