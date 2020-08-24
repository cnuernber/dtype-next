(ns tech.v3.datatype.argtypes
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [tech.v3.datatype.protocols PToReader]
           [java.util RandomAccess]))


(defn arg-type
  [arg]
  (cond
    (or (instance? Number arg)
        (instance? Boolean arg)
        (string? arg)
        (nil? arg))
    :scalar
    (or (instance? PToReader arg)
        (instance? RandomAccess arg)
        (dtype-proto/convertible-to-reader? arg))
    :reader
    (instance? Iterable arg)
    :iterable
    :else
    :scalar))
