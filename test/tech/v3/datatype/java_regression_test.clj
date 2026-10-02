(ns tech.v3.datatype.java-regression-test
  "Regression tests for bugs in the java/ support classes."
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.tensor :as dtt])
  (:import [tech.v3.datatype Buffer ObjectReader BinaryPredicate
            BinaryPredicates$DoubleBinaryPredicate UByteSubBuffer]
           [java.util.stream StreamSupport]
           [java.util.function ToLongFunction]))


(deftest buffer-default-write-double
  (let [store (object-array 3)
        b (reify Buffer
            (lsize [_] 3)
            (readObject [_ i] (aget store i))
            (writeObject [_ i v] (aset store i v)))]
    (.writeDouble b 1 1.5)
    (is (= 1.5 (aget store 1)))))


(deftest object-reader-write-throws
  (let [r (reify ObjectReader
            (lsize [_] 1)
            (readObject [_ _i] :a))]
    (is (thrown? RuntimeException (.writeObject r 0 :b)))))


(deftest binary-predicate-defaults
  (let [bp (reify BinaryPredicate
             (binaryObject [_ a b] (< (double a) (double b))))]
    (is (true? (.binaryDouble bp 1.0 2.0)))
    (is (false? (.binaryDouble bp 2.0 1.0))))
  (let [dbp (reify BinaryPredicates$DoubleBinaryPredicate
              (binaryDouble [_ a b] (< a b)))]
    (is (true? (.binaryObject dbp 1 2)))
    (is (false? (.binaryObject dbp 2 1))))
  (let [lt (binary-pred/ifn->binary-predicate (fn [^double a ^double b] (< a b)) :lt)]
    (is (true? (.binaryObject lt 1.0 2.0)))))


(deftest buffer-parallel-spliterator
  (let [n 10000
        b (reify Buffer
            (lsize [_] n)
            (readObject [_ i] i))
        stream #(StreamSupport/stream (.spliterator b) true)]
    (is (= n (.count ^java.util.stream.Stream (stream))))
    (is (= (quot (* n (dec n)) 2)
           (-> ^java.util.stream.Stream (stream)
               (.mapToLong (reify ToLongFunction (applyAsLong [_ x] (long x))))
               (.sum))))
    (is (= (vec (range n)) (vec (.toArray ^java.util.stream.Stream (stream)))))))


(deftest ubyte-sub-buffer-move
  (let [data (byte-array [0 1 2 3 4 5 6 7])
        sub (UByteSubBuffer. data 4 8 nil)]
    (.move sub 0 1 2)
    (is (= [4 4 5 7] (vec sub)))
    (is (= [0 1 2 3] (vec (take 4 data))))
    (is (thrown? Exception (.move sub 0 3 2)))))


(deftest tensor-nth-not-found
  (let [t (dtt/->tensor [[1 2] [3 4]])]
    (is (= :nf (nth t 2 :nf)))
    (is (= [3 4] (vec (nth t 1 :nf))))))

