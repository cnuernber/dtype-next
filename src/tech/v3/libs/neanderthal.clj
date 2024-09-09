(ns tech.v3.neanderthal)


(def ^:private impl
  {:tensor->matrix (requiring-resolve 'tech.v3.libs.neanderthal-pre-48/tensor->matrix)
   :dtype->native-factory (requiring-resolve 'tech.v3.libs.neanderthal-pre-48/datatype->native-factory)})


(defn tensor->matrix
  ([tens] ((get impl :tensor->matrix) tens))
  ([tens layout datatype]
   ((get impl :tensor->matrix) tens layout datatype)))

(defn datatype->native-factory
  [dtype]
  ((get impl :datatype->native-factory) dtype))
