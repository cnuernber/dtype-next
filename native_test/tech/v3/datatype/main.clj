(ns tech.v3.datatype.main
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.ffi.graalvm-runtime])
  (:import [tech.v3.datatype GraalNativeGen])
  (:gen-class))


(set! *warn-on-reflection* true)


;; (with-bindings {#'*compile-path* "generated_classes"}
;;   (do (graalvm/define-library
;;         {:memset {:rettype :pointer
;;                   :argtypes [['buffer :pointer]
;;                              ['byte-value :int32]
;;                              ['n-bytes :size-t]]}}
;;         nil
;;         {:header-files ["<string.h>"]
;;          :classname 'tech.v3.datatype.GraalNativeGen})
;;       nil))

(import 'tech.v3.datatype.GraalNativeGen)

(defn -main
  [& args]
  (let [nbuf (dtype/make-container :native-heap :float32 (range 10))
        inst (GraalNativeGen.)]
    (.memset inst nbuf 0 40)
    (println nbuf))
  0)
