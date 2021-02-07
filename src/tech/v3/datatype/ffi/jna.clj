(ns tech.v3.datatype.ffi-jna
  (:require [tech.v3.datatype.ffi :as ffi])
  (:import [com.sun.jna NativeLibrary Pointer]))


(extend-type Pointer
  ffi/PToPointer
  (convertible-to-pointer? [item] true)
  (->pointer [item] (ffi/->Pointer (Pointer/nativeValue item))))


(defn library-instance?
  [item]
  (instance? NativeLibrary item))


(defn load-library
  ^Nativelibrary [libname]
  (if (instance? NativeLibrary libname)
    libname
    (NativeLibrary/getInstance (str libname))))


(defn find-symbol
  [libname symbol-name]
  (-> (load-library libname)
      (.getGlobalVariableAddress (str symbol-name))))


(def ffi-fns {:library-instance? library-instance?
              :load-library load-library
              :find-symbol find-symbol})
