(ns make-graal
  (:require [libc :as libc]
            [tech.v3.datatype.ffi.graalvm :as graalvm]))


(defn make-bindings
  []
  (.mkdir (java.io.File. "generated-classes"))
    (with-bindings {#'*compile-path* "generated-classes"}
      (graalvm/define-library
        libc/fn-defs
        nil
        {:classname 'libc.GraalBindings
         :headers ["<string.h>"]
         :instantiate? true})))


(make-bindings)
