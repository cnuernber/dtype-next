(ns tech.v3.datatype.argtypes
  (:require [tech.v3.datatype.protocols :as dtype-proto])
  (:import [tech.v3.datatype.protocols PToReader]
           [tech.v3.datatype NDBuffer]
           [java.util RandomAccess Map]))


(defn arg-type
  "Return the type of a thing.  Types could be:
  :scalar
  :iterable
  :reader
  :tensor (reader with more than 1 dimension)"
  [arg]
  (cond
    (or (instance? Number arg)
        (instance? Boolean arg)
        (instance? Map arg)
        (string? arg)
        (nil? arg))
    :scalar
    (or (instance? PToReader arg)
        (instance? RandomAccess arg)
        (dtype-proto/convertible-to-reader? arg))
    ;;I thought this was clever but we need to just ask if this
    ;;is a tensor directly
    (if (and (instance? NDBuffer arg)
             (not= 1 (count (dtype-proto/shape arg))))
      :tensor
      :reader)
    (instance? Iterable arg)
    :iterable
    :else
    :scalar))


(defn reader-like?
  "Returns true if this argument type is convertible to a reader."
  [argtype]
  (or (= argtype :reader)
      (= argtype :tensor)))
