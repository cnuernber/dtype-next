(ns tech.v3.datatype.reductions
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [tech.v3.datatype BinaryOperator]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn commutative-binary-double
  ^double [rdr ^BinaryOperator op]
  (let [rdr (dtype-base/->reader rdr)]
    (double
     (parallel-for/indexed-map-reduce
      (.lsize rdr)
      (fn [^long start-idx ^long group-len]
        (let [end-idx (+ start-idx group-len)]
          (loop [idx (inc start-idx)
                 accum (.readDouble rdr start-idx)]
            (if (< idx end-idx)
              (recur (unchecked-inc idx) (.binaryDouble
                                          op accum
                                          (.readDouble rdr idx)))
              accum))))
      (partial reduce op)))))


(defn commutative-binary-long
  ^long [rdr ^BinaryOperator op]
  (let [rdr (dtype-base/->reader rdr)]
    (long
     (parallel-for/indexed-map-reduce
      (.lsize rdr)
      (fn [^long start-idx ^long group-len]
        (let [end-idx (+ start-idx group-len)]
          (loop [idx (inc start-idx)
                 accum (.readLong rdr start-idx)]
            (if (< idx end-idx)
              (recur (unchecked-inc idx) (.binaryLong
                                          op accum
                                          (.readLong rdr idx)))
              accum))))
      (partial reduce op)))))


(defn commutative-binary-object
  [rdr op]
  (let [rdr (dtype-base/->reader rdr)]
    (parallel-for/indexed-map-reduce
     (.lsize rdr)
     (fn [^long start-idx ^long group-len]
       (let [end-idx (+ start-idx group-len)]
         (loop [idx (inc start-idx)
                accum (.readObject rdr start-idx)]
           (if (< idx end-idx)
             (recur (unchecked-inc idx) (op accum
                                         (.readObject rdr idx)))
             accum))))
     (partial reduce op))))


(defn commutative-binary-reduce
  [rdr op]
  (if-let [rdr (dtype-base/->reader rdr)]
    (if (instance? BinaryOperator op)
      (let [rdr-dtype (dtype-base/elemwise-datatype rdr)]
        (cond
          (casting/integer-type? rdr-dtype)
          (commutative-binary-long rdr op)
          (casting/float-type? rdr-dtype)
          (commutative-binary-double rdr op)
          :else
          (commutative-binary-object rdr op)))
      (commutative-binary-object rdr op))
    ;;Clojure core reduce is actually damn fast and
    (reduce op rdr)))
