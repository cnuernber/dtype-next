(ns tech.v3.datatype.main
  (:require [tech.v3.datatype.ffi.graalvm :as graalvm]
            [tech.v3.datatype :as dtype])
  (:gen-class))

(set! *warn-on-reflection* true)


(defonce gen-lib (graalvm/define-library
                   {:memset {:rettype :pointer
                             :argtypes [['buffer :pointer]
                                        ['byte-value :int32]
                                        ['n-bytes :size-t]]}}
                   [:memmove]
                   {:header-files ["string.h"]
                    :classname 'tech.v3.datatype.native.NativeFns}))

(import 'tech.v3.datatype.native.NativeFns)

(defn -main
  [& args]
  (let [nbuf (dtype/make-container :native-heap :float32 (range 10))
        nfns (NativeFns.)]
    (.memset nfns nbuf 0 40)
    (println (vec nbuf)))
  0)
