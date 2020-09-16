(ns tech.v3.datatype.streams-test
  (:require [tech.v3.datatype :as dtype]
            [clojure.test :refer [deftest is]]))


(defn typed-stream-sum
  [rdr]
  (case (dtype/get-datatype rdr)
    :boolean (-> (dtype/->reader rdr)
                 (.typedStream)
                 (.sum))
    :int8 (-> (dtype/->reader rdr)
              (.typedStream)
              (.sum))
    :int16 (-> (dtype/->reader rdr)
               (.typedStream)
               (.sum))
    :int32 (-> (dtype/->reader rdr)
               (.typedStream)
               (.sum))
    :int64 (-> (dtype/->reader rdr)
               (.typedStream)
               (.sum))
    :float32 (-> (dtype/->reader rdr)
                 (.typedStream)
                 (.sum))
    :float64 (-> (dtype/->reader rdr)
                 (.typedStream)
                 (.sum))
    (-> (dtype/->reader rdr)
        (.typedStream)
        (.reduce (reify java.util.function.BinaryOperator
                   (apply [this lhs rhs]
                     (+ lhs rhs)))))))


(deftest basic-datatype-streams
  (let [dtype-list [:int8 :int16 :int32 :int64
                    :float32 :float64]
        readers (concat (->> dtype-list
                             (map #(dtype/make-container :java-array % (range 10)))
                             (map dtype/->reader))
                        [(dtype/->reader (vec (range 10)))
                         (dtype/->reader (boolean-array (map even? (range 10))))])]
    (is (= [45
            45
            45
            45
            45.0
            45.0
            (java.util.Optional/of 45)
            5]
           (vec (mapv typed-stream-sum readers))))))
