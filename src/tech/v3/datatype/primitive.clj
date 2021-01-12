(ns tech.v3.datatype.primitive
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.const-reader :refer [const-reader]])
  (:import [tech.v3.datatype ObjectReader]))



(defmacro implement-scalar-primitive
  [cls prim-cls datatype]
  `(do
     (.put casting/class->datatype-map ~cls ~datatype)
     (.put casting/class->datatype-map ~prim-cls ~datatype)
     (clojure.core/extend
          ~cls
        dtype-proto/PDatatype
        {:datatype (fn [item#] ~datatype)}
        dtype-proto/PElemwiseDatatype
        {:elemwise-datatype (fn [item#] ~datatype)}
        dtype-proto/PECount
        {:ecount (fn [item#] 1)}
        dtype-proto/PConstantTimeMinMax
        {:has-constant-time-min-max? (constantly true)
         :constant-time-min identity
         :constant-time-max identity}
        dtype-proto/PToReader
        ;;Reader conversion of primitives is inefficient so we allow it
        ;;but do not advertise it
        {:convertible-to-reader? (constantly false)
         :->reader (fn [item#]
                     (const-reader item# 1))})))


(implement-scalar-primitive Boolean Boolean/TYPE :boolean)
(implement-scalar-primitive Byte Byte/TYPE :int8)
(implement-scalar-primitive Short Short/TYPE :int16)
(implement-scalar-primitive Character Character/TYPE :char)
(implement-scalar-primitive Integer Integer/TYPE :int32)
(implement-scalar-primitive Long Long/TYPE :int64)
(implement-scalar-primitive Float Float/TYPE :float32)
(implement-scalar-primitive Double Double/TYPE :float64)
