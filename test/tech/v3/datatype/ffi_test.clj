(ns tech.v3.datatype.ffi-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.ffi :as dtype-ffi]
            [clojure.test :refer [deftest is]]))



(deftest jna-ffi-test
  (dtype-ffi/set-ffi-impl! :jna)
  (let [libmem-cls (dtype-ffi/define-library
                     {'memset {:rettype :pointer
                               :argtypes {'buffer :pointer
                                          'byte-value :int32
                                          'n-bytes :size-t}}
                      'memcpy {:rettype :pointer
                               ;;dst src size-t
                               :argtypes {'dst :pointer
                                          'src :pointer
                                          'n-bytes :size-t}}})
        libmem-inst (dtype-ffi/instantiate-class libmem-cls nil)
        memcpy (libmem-inst 'memcpy)
        memset (libmem-inst 'memset)
        first-buf (dtype/make-container :native-heap :float32
                                        (range 10))
        second-buf (dtype/make-container :native-heap :float32
                                         (range 10))]
    (libmem-inst 'memcpy first-buf 0 40)
    (is (dfn/equals first-buf (vec (repeat 10 0.0))))))
