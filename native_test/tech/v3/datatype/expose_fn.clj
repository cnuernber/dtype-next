(ns tech.v3.datatype.expose-fn
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.native-buffer :as native-buffer])
  (:import [tech.v3.datatype.ffi Pointer]))


(defn add-two-nums
  [num-a num-b]
  (+ num-a num-b))


(defn allocate-buffer
  [len]
  ;;setup buffer to be manually freed.
  (when-not (== 0 len)
    (dtype/make-container :native-heap :int8 {:resource-type nil} len)))


(defn set-buffer
  [buffer len val]
  (-> (native-buffer/wrap-address (.address ^Pointer buffer) len nil)
      (dtype/set-constant! val)))


(defn free-buffer
  [buffer]
  (when (and buffer (not= 0 (.address ^Pointer buffer)))
    (native-buffer/free (native-buffer/wrap-address buffer 1 nil))))



(comment
  (do
    (require '[tech.v3.datatype.ffi.graalvm :as graalvm])
    (def cls-def
      (with-bindings {#'*compile-path* "library_test/classes"}
        (graalvm/expose-clojure-functions
         {#'add-two-nums {:rettype :int64
                          :argtypes [['lhs :int64]
                                     ['rhs :int64]]}
          #'allocate-buffer {:rettype :pointer?
                             :argtypes [['len :int64]]}
          #'set-buffer {:rettype :void
                        :argtypes [['buffer :pointer]
                                   ['len :int64]
                                   ['val :int8]]}
          #'free-buffer {:rettype :void
                         :argtypes [['buffer :pointer?]]}

          }
         'libdtype.ExposeFnTest
         {:instantiate? true})))
    ))
