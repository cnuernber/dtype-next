(ns tech.v3.datatype.main
  (:require [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.errors :as errors]
            ;; [tech.v3.datatype.array-buffer]
            [tech.v3.datatype.native-buffer]
            [tech.v3.datatype.list]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-copy-make-container]
            [tech.v3.datatype.clj-range]

            ;; [tech.v3.datatype.functional]
            ;; [tech.v3.datatype.copy-raw-to-item]
            ;; [tech.v3.datatype.primitive]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.nio-buffer]
            [tech.v3.datatype.io-indexed-buffer :as io-idx-buf]
            ;; [tech.v3.datatype.io-concat-buffer]
            ;; [tech.v3.datatype.unary-op :as unary-op]
            ;; [tech.v3.datatype.unary-pred :as unary-pred]
            ;; [tech.v3.datatype.binary-op :as binary-op]
            ;; [tech.v3.datatype.binary-pred :as binary-pred]
            ;; [tech.v3.datatype.emap :as emap]
            ;; [tech.v3.datatype.export-symbols :refer [export-symbols]]
            )
  (:gen-class))

(set! *warn-on-reflection* true)

(defn -main
  [& args]
  (let [data (array-buffer/array-buffer (float-array (range 10)))]
    (println data))
  0)
