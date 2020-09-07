(ns tech.v3.datatype.list-test
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.list :as dtype-list]
            [clojure.test :refer [deftest is]])
  (:import [tech.v3.datatype PrimitiveList]))

(set! *warn-on-reflection* true)

(comment
  (do
    (require '[criterium.core :as crit])
    (def double-data (double-array (range 100000)))
    (defn benchmark-list-add
      []
      (let [^PrimitiveList container (dtype-cmc/make-container :list :float64 0)
            ;;Makes a reader out of the double array
            rdr (dtype-base/->reader double-data)]
        ;;Addall checks to see if item is convertible to a raw buffer
        ;;which readers derived from arrays are.  Then uses optimized
        ;;dtype/copy pathway
        (.addAll container rdr)
        container))
    )
  )
