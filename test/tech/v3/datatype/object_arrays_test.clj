(ns tech.v3.datatype.object-arrays-test
  (:require [clojure.test :refer :all]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.list]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op])
  (:import [java.util List UUID]))


(deftest boolean-array-test
  (let [test-ary (dtype/make-container :boolean 5)]
    (is (= [false false false false false]
           (dtype/->vector test-ary)))
    (is (= :boolean
           (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 2 true)
    (is (= [false false true false false]
           (dtype/->vector test-ary)))
    (is (= [false false true false false]
           (-> (dtype/copy! test-ary (dtype/make-container :boolean 5))
               (dtype/->vector))))))


(deftest string-array-test
  (let [test-ary (dtype/make-container :string 5)]
    (is (= [nil nil nil nil nil]
           (dtype/->vector test-ary)))
    (is (= :string (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 3 "hi")
    (is (= [nil nil nil "hi" nil]
           (dtype/->vector test-ary)))

    (let [sub-buf (dtype-proto/sub-buffer test-ary 2 3)]
      (is (= :string (dtype/get-datatype sub-buf)))
      (dtype/set-value! sub-buf 0 "bye!")
      (is (= [nil nil "bye!" "hi" nil]
             (dtype/->vector test-ary)))
      (is (= ["bye!" "hi" nil]
             (dtype/->vector sub-buf)))))
  (let [test-ary (dtype/make-container :string ["a" "b" "c"])
        test-rdr (->> test-ary
                      (unary-op/reader
                       #(.concat ^String % "_str")
                       :string))
        test-iter (->> test-ary
                       (unary-op/iterable
                        #(.concat % "_str")
                        :string))]
    (is (= :string (dtype/get-datatype test-rdr)))
    (is (= ["a_str" "b_str" "c_str"]
           (vec test-rdr)))
    (is (= :string (dtype/get-datatype test-iter)))
    (is (= ["a_str" "b_str" "c_str"]
           (vec test-iter))))
  (let [test-ary (dtype/make-container :string ["a" "b" "c"])
        test-rdr (binary-op/reader
                  #(str %1 "_" %2)
                  :string
                  test-ary test-ary)
        test-iterable (binary-op/iterable
                       #(str %1 "_" %2)
                       :string
                       test-ary test-ary)]
    (is (= :string (dtype/get-datatype test-rdr)))
    (is (= :string (dtype/get-datatype test-iterable)))
    (is (= ["a_a" "b_b" "c_c"]
           (vec test-rdr)))
    (is (= ["a_a" "b_b" "c_c"]
           (vec test-iterable)))))


(deftest new-string-container
  (is (= ["a_str" "b_str" "c_str"]
         (->> (dtype/make-container :string ["a" "b" "c"])
              (unary-op/reader
               #(.concat % "_str")
               :string)
              (dtype/make-container :java-array :string)
              vec))))


(deftest object-array-test
  (let [test-ary (dtype/make-container Object 5)]
    (is (= [nil nil nil nil nil]
           (dtype/->vector test-ary)))
    (is (= :object (dtype/get-datatype test-ary)))
    (dtype/set-value! test-ary 3 "hi")
    (is (= [nil nil nil "hi" nil]
           (dtype/->vector test-ary)))
    (let [sub-buf (dtype-proto/sub-buffer test-ary 2 3)]
      (is (= :object (dtype/get-datatype sub-buf)))
      (dtype/set-value! sub-buf 0 "bye!")
      (is (= [nil nil "bye!" "hi" nil]
             (dtype/->vector test-ary)))
      (is (= ["bye!" "hi" nil]
             (dtype/->vector sub-buf)))))
  (let [test-ary (into-array Object (repeat 10 (set (range 10))))]
    (is (= 10 (dtype/ecount test-ary)))))


(deftest generic-list-test
  (let [^List data (dtype/make-container :list :keyword [:a :b :c :d :e])]
    (.addAll data [:f :g :h :i])
    (is (= [:a :b :c :d :e :f :g :h :i]
           (vec (.toArray data))))))


(deftest uuid-test
  (let [test-uuid (UUID/randomUUID)
        uuid-ary (dtype/make-container :java-array :uuid (repeat 5 test-uuid))
        uuid-list (dtype/make-container :list :uuid (repeat 5 test-uuid))]
    (is (= :uuid (dtype/get-datatype uuid-ary)))
    (is (thrown? Throwable (dtype/set-value! uuid-ary 2 "hey")))
    (is (= (vec (repeat 5 test-uuid))
           (vec uuid-ary)))
    (is (= :uuid (dtype/get-datatype uuid-list)))
    (is (thrown? Throwable (dtype/set-value! uuid-list 2 "hey")))
    (is (= (vec (repeat 5 test-uuid))
           (vec uuid-list)))
    (is (instance? List uuid-list))))
