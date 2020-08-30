(ns tech.v3.datatype.reductions
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryOperator IndexReduction DoubleReduction]
           [java.util List]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn commutative-binary-double
  ^double [^BinaryOperator op rdr]
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
  ^long [^BinaryOperator op rdr]
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
  [op rdr]
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
  [op rdr]
  (if-let [rdr (dtype-base/->reader rdr)]
    (if (instance? BinaryOperator op)
      (let [rdr-dtype (dtype-base/elemwise-datatype rdr)]
        (cond
          (casting/integer-type? rdr-dtype)
          (commutative-binary-long op rdr)
          (casting/float-type? rdr-dtype)
          (commutative-binary-double op rdr)
          :else
          (commutative-binary-object op rdr)))
      (commutative-binary-object op rdr))
    ;;Clojure core reduce is actually pretty good!
    (reduce op rdr)))


(defn double-reductions
  "Given a map of name->reducer of DoubleReduction implementations and a rdr
  do an efficient two-level parallelized reduction and return the results in
  a map of name->finalized-result."
  [reducer-map rdr]
  (let [^List reducer-names (keys reducer-map)
        ^List reducers (vec (vals reducer-map))
        n-reducers (.size reducers)
        rdr (dtype-base/->reader rdr)
        n-elems (.lsize rdr)]
    (let [result
          (parallel-for/indexed-map-reduce
           n-elems
           (fn [^long start-idx ^long n-groups]
             (let [end-idx (long (+ start-idx n-groups))
                   intermediate-values (double-array (.size reducers))]
               (let [dval (.readDouble rdr start-idx)]
                 (dotimes [reducer-idx n-reducers]
                   (aset intermediate-values reducer-idx
                         (.initialize ^DoubleReduction (.get reducers reducer-idx)
                                      dval))))
               (loop [idx (unchecked-inc start-idx)]
                 (when (< idx end-idx)
                   (let [dval (.readDouble rdr idx)]
                     (dotimes [reducer-idx n-reducers]
                       (aset intermediate-values reducer-idx
                             (.update ^DoubleReduction (.get reducers reducer-idx)
                                      (aget intermediate-values reducer-idx)
                                      dval))))
                   (recur (unchecked-inc idx))))
               intermediate-values))
           (partial reduce (fn [^doubles lhs ^doubles rhs]
                             (dotimes [reducer-idx n-reducers]
                               (aset lhs reducer-idx
                                     (.merge
                                      ^DoubleReduction (.get reducers reducer-idx)
                                      (aget lhs reducer-idx)
                                      (aget rhs reducer-idx))))
                             lhs)))]
      (->> (map (fn [k ^DoubleReduction r v]
                  [k (.finalize r ^Double v n-elems)])
                reducer-names reducers result)
           (into {})))))
