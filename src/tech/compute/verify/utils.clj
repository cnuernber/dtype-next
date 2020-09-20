(ns tech.compute.verify.utils
  (:require [tech.resource :as resource]
            [clojure.test :refer :all]
            [tech.v2.datatype.casting :as casting]
            [tech.compute :as compute]
            [tech.compute.context :as compute-ctx])
  (:import [java.math BigDecimal MathContext]))


(defn test-wrapper
  [test-fn]
  (resource/stack-resource-context
    ;;Turn on if you want much slower tests.
    (test-fn)))


(defmacro with-default-device-and-stream
  [driver & body]
  `(resource/stack-resource-context
    (compute-ctx/with-context
      {:driver ~driver})
     ~@body))


(def ^:dynamic *datatype* :float64)


(defmacro datatype-list-tests
  [datatype-list test-name & body]
  `(do
     ~@(for [datatype datatype-list]
         (do
           `(deftest ~(symbol (str test-name "-" (name datatype)))
              (with-bindings {#'*datatype* ~datatype}
                ~@body))))))



(defmacro def-double-float-test
  [test-name & body]
  `(datatype-list-tests [:float64 :float32] ~test-name ~@body))


(defmacro def-int-long-test
  [test-name & body]
  `(datatype-list-tests [:int32 :uint32 :int64 :uint64]
                        ~test-name
                        ~@body))


(defmacro def-all-dtype-test
  [test-name & body]
  `(datatype-list-tests ~casting/numeric-types ~test-name ~@body))


(defmacro def-all-dtype-exception-unsigned
  "Some platforms can detect unsigned errors."
  [test-name & body]
  `(do
     (datatype-list-tests ~casting/host-numeric-types ~test-name ~@body)
     (datatype-list-tests ~casting/unsigned-int-types ~test-name
                          (is (thrown? Throwable
                                       ~@body)))))
