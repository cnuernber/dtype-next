(ns tech.v3.datatype.main
  (:require [tech.v3.datatype.expose-fn]
            [tech.v3.datatype.ffi.graalvm-runtime :as graalvm-runtime])
  (:import [clojure.lang RT])
  (:gen-class))


(set! *warn-on-reflection* true)


(comment
  (do
    (require '[tech.v3.datatype.ffi.graalvm :as graalvm])
    (def lib-def
      (with-bindings {#'*compile-path* "generated_classes"}
        (do (graalvm/define-library
              {:memset {:rettype :pointer
                        :argtypes [['buffer :pointer]
                                   ['byte-value :int32]
                                   ['n-bytes :size-t]]}}
              [:M_PI]
              {:header-files ["<string.h>" "<math.h>"]
               :classname 'tech.v3.datatype.GraalNativeGen
               :instantiate? true})))))
  )

(defn -main
  [& args]
  0)
