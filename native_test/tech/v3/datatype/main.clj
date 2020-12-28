(ns tech.v3.datatype.main
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.tensor :as dtt]
            [tech.v3.jna :as jna])
  (:import [tech.v3.datatype DirectMapped])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn -main
  [& args]
  (println "loading jna jni lib")
  (System/load (str (System/getProperty "user.dir") "/libs/libjnidispatch.so"))
  (println "attempting to load native c lib")
  (let [ld (dtype-dt/local-date)
        native-lib (jna/load-library "c")
        data (byte-array 10)]
    (com.sun.jna.Native/register DirectMapped native-lib)
    (DirectMapped/memset (java.nio.ByteBuffer/wrap data) -1 10)
    (println (vec data)))
  0)
