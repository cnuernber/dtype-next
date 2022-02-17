(ns tech.v3.datatype.codegen
  (:require [tech.v3.datatype.export-symbols :as export-symbols]
            [tech.v3.datatype.errors :as errors]
            [clojure.tools.logging :as log])
  (:gen-class))


(defn -main
  [& args]

  (log/info "Generating datatype api files")

    (export-symbols/write-api! 'tech.v3.datatype.functional-api
                             'tech.v3.datatype.functional
                             "src/tech/v3/datatype/functional.clj"
                             '[+ - / *
                               <= < >= >
                               identity
                               min max
                               bit-xor bit-and bit-and-not bit-not bit-set bit-test
                               bit-or bit-flip bit-clear
                               bit-shift-left bit-shift-right unsigned-bit-shift-right
                               quot rem cast not and or
                               neg? even? zero? odd? pos?])

  (export-symbols/write-api! 'tech.v3.datatype.datetime-api
                             'tech.v3.datatype.datetime
                             "src/tech/v3/datatype/datetime.clj"
                             nil)

  (export-symbols/write-api! 'tech.v3.datatype-api
                             'tech.v3.datatype
                             "src/tech/v3/datatype.clj"
                             ['cast 'reverse])


  (export-symbols/write-api! 'tech.v3.tensor-api
                             'tech.v3.tensor
                             "src/tech/v3/tensor.clj"
                             nil)


  (log/info "Finished generating api files"))
