(ns graal-main
  (:require [libc :as libc]
            [tech.v3.datatype.ffi :as dt-ffi]
            [tech.v3.datatype :as dtype]
            ;;required for generated class to work correctly
            [tech.v3.datatype.ffi.graalvm-runtime])
  (:import [libc GraalBindings])
  (:gen-class))


(defn -main
  [& args]
  (let [libinst (libc.GraalBindings.)
        _ (dt-ffi/library-singleton-set-instance! libc/lib libinst)
        nbuf (dtype/make-container :native-heap :float32 (range 10))]
    (println "before memset" nbuf)
    (libc/memset nbuf 0 40)
    (println "after memset" nbuf)
    0))
