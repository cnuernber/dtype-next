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
    (if (= 1 (count (dtype-proto/shape arg)))
      :reader
      :tensor)
    (instance? Iterable arg)
    :iterable
    :else
    :scalar))
