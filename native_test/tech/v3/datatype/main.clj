(ns tech.v3.datatype.main
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.ffi :as dt-ffi]
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

(import 'tech.v3.datatype.GraalNativeGen)

(defn -main
  [& args]
  (let [nbuf (dtype/make-container :native-heap :float32 (range 10))
        inst (GraalNativeGen.)]
    (.memset inst nbuf 0 40)
    (println (graalvm-runtime/ptr-value nbuf))
    (println nbuf)
    (println (dt-ffi/string->c "hey")))

  0)
