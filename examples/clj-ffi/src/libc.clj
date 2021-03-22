(ns libc
  (:require [tech.v3.datatype.ffi :as dt-ffi]))


(def fn-defs
  {:memset {:rettype :pointer
            :argtypes [['buffer :pointer]
                       ['byte-value :int32]
                       ['n-bytes :size-t]]}
   :memcpy {:rettype :pointer
            ;;dst src size-t
            :argtypes [['dst :pointer]
                       ['src :pointer]
                       ['n-bytes :size-t]]}})


(defonce lib (dt-ffi/library-singleton #'fn-defs))
(dt-ffi/library-singleton-reset! lib)
(defn- find-fn
  [fn-name]
  (dt-ffi/library-singleton-find-fn lib fn-name))

(dt-ffi/define-library-functions
  libc/fn-defs
  find-fn
  nil)
