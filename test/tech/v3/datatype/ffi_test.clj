(ns tech.v3.datatype.ffi-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.ffi :as dtype-ffi]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [clojure.test :refer [deftest is]]
            [clojure.tools.logging :as log])
  (:import [tech.v3.datatype.ffi Pointer]))


(defn generic-define-library
  []
  (let [libmem-def (dtype-ffi/define-library
                     {:memset {:rettype :pointer
                               :argtypes [['buffer :pointer]
                                          ['byte-value :int32]
                                          ['n-bytes :size-t]]}
                      :memcpy {:rettype :pointer
                               ;;dst src size-t
                               :argtypes [['dst :pointer]
                                          ['src :pointer]
                                          ['n-bytes :size-t]]}
                      :qsort {:rettype :void
                              :argtypes [['data :pointer]
                                         ['nitems :size-t]
                                         ['item-size :size-t]
                                         ['comparator :pointer]]}})
        ;;nil meaning find the symbols in the current process
        libmem-inst (dtype-ffi/instantiate-library libmem-def nil)
        libmem-fns @libmem-inst
        memcpy (:memcpy libmem-fns)
        memset (:memset libmem-fns)
        qsort (:qsort libmem-fns)
        comp-iface-def (dtype-ffi/define-foreign-interface :int32 [:pointer :pointer])
        comp-iface-inst (dtype-ffi/instantiate-foreign-interface
                         comp-iface-def
                         (fn [^Pointer lhs ^Pointer rhs]
                           (let [lhs (.getDouble (native-buffer/unsafe) (.address lhs))
                                 rhs (.getDouble (native-buffer/unsafe) (.address rhs))]
                             (Double/compare lhs rhs))))
        comp-iface-ptr (dtype-ffi/foreign-interface-instance->c
                        comp-iface-def
                        comp-iface-inst)
        first-buf (dtype/make-container :native-heap :float32 (range 10))
        second-buf (dtype/make-container :native-heap :float32 (range 10))
        dbuf (dtype/make-container :native-heap :float64 (shuffle (range 100)))]
    (memset first-buf 0 40)
    (memcpy second-buf first-buf 40)
    (qsort dbuf (dtype/ecount dbuf) Double/BYTES comp-iface-ptr)
    (is (dfn/equals first-buf (vec (repeat 10 0.0))))
    (is (dfn/equals second-buf (vec (repeat 10 0.0))))
    (is (dfn/equals dbuf (range 100)))
    (is (= (.findSymbol libmem-inst "qsort")
           (.findSymbol libmem-inst "qsort")))))


(deftest jna-ffi-test
  (dtype-ffi/set-ffi-impl! :jna)
  (generic-define-library))


(if (dtype-ffi/jdk-ffi?)
  (deftest mmodel-ffi-test
    (dtype-ffi/set-ffi-impl! :jdk)
    (generic-define-library))
  (log/warn "JDK-16 FFI pathway not tested."))
