(ns tech.v3.datatype
  (:require [tech.v3.datatype.array-buffer]
            [tech.v3.datatype.native-buffer]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-copy-make-container]
            [tech.v3.datatype.clj-range]
            [tech.v3.datatype.list]
            [tech.v3.datatype.functional]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]))


(export-symbols tech.v3.datatype.base
                elemwise-datatype
                ecount
                ->io
                ->reader
                ->writer
                ->array-buffer
                ->native-buffer
                sub-buffer)

(export-symbols tech.v3.datatype.copy-make-container
                make-container
                copy!)


(defn ->vector
  "Convert a datatype thing to a vector"
  [item]
  (vec (->reader item)))
