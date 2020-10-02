(ns tech.v3.datatype.datetime-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.datatype :as dtype]))


(deftest simple-packing
  (let [ld (dtype-dt/local-date)]
    (let [data-list (dtype/make-container :list :packed-local-date 0)]
      (.addObject data-list ld)
      (is (= [ld]
             (vec data-list)))
      (is (= [ld]
             (vec (dtype/->array data-list)))))
    (let [data-buf (dtype/make-container :jvm-heap :packed-local-date 5)]
      (is (= (vec (repeat 5 nil))
             (vec data-buf)))
      (dtype/set-value! data-buf 1 ld)
      (is (= (vec [nil ld nil nil nil])
             (vec data-buf))))))
