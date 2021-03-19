(ns tech.v3.datatype.ffi.graalvm-runtime
  (:require [tech.v3.datatype.ffi.ptr-value :as ptr-value]
            [tech.v3.datatype.errors :as errors])
  (:import [org.graalvm.word WordFactory WordBase]
           [org.graalvm.nativeimage.c.type VoidPointer]
           [tech.v3.datatype.ffi Pointer]))



(defn ptr-value
  ^Pointer [item]
  (long (ptr-value/ptr-value item)))


(defn ptr-value-q
  ^Pointer [item]
  (ptr-value/ptr-value? item))


(defn find-library-symbol
  ^Pointer [symbol-name symbol-map]
  (if-let [retval (get symbol-map (keyword symbol-name))]
    (tech.v3.datatype.ffi.Pointer/constructNonZero (long retval))
    (errors/throwf "Failed to find symbol \"%s\"" symbol-name)))
