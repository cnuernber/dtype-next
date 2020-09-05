(ns tech.v3.datatype.reductions-test
  (:require [tech.v3.datatype.reductions :as reductions]
            [tech.v3.datatype.unary-op :as unop]
            [tech.v3.datatype.binary-op :as binop]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [primitive-math :as pmath]
            [clojure.test :refer [deftest is]])
  (:import [tech.v3.datatype DoubleReduction
            DoubleConsumers$SummationConsumer
            DoubleConsumers$DoubleConsumerResult]
           [java.util Spliterator Spliterator$OfDouble]
           [java.util.function DoubleConsumer]))

(deftest summation-reductions
  (let [data (double-array (range 100))
        nan-data (double-array (range 100))
        _ (aset nan-data 50 Double/NaN)
        answer (double (reduce + data))
        nan-answer (double (- answer 50))]
    (is (= {:n-elems 100 :data {:sum answer}}
           (reductions/double-reductions {:sum :+} data
                                         {:nan-strategy-or-predicate :keep})))
    (is (= {:n-elems 99 :data {:sum nan-answer}}
           (reductions/double-reductions {:sum :+}  nan-data)))
    (is (thrown? Throwable
                 (reductions/double-reductions
                  {:sum :+} nan-data
                  {:nan-strategy-or-predicate :exception})))
    (is (= {:n-elems 99 :data {:sum nan-answer}}
           (reductions/double-reductions {:sum (:identity unop/builtin-ops)}
                                         nan-data)))
    (is (= {:n-elems 99 :data {:sum nan-answer}}
           (reductions/double-reductions {:sum (:+ binop/builtin-ops)}
                                         nan-data)))

    (is (= {:n-elems 99 :data {:sum nan-answer}}
           (reductions/double-reductions {:sum (reify DoubleReduction
                                                  (update [this lhs rhs]
                                                    (+ lhs rhs)))}
                                         nan-data)))

    (is (= {:n-elems 100 :data {:sum1 answer
                                :sum2 answer
                                :sum3 answer
                                :sum4 answer}}
           (reductions/double-reductions {:sum1 :+
                                          :sum2 (:identity unop/builtin-ops)
                                          :sum3 (:+ binop/builtin-ops)
                                          :sum4 (reify DoubleReduction
                                                  (update [this lhs rhs]
                                                    (+ lhs rhs)))}
                                         data
                                         {:nan-strategy-or-predicate :keep})))
    (is (= {:n-elems 99 :data {:sum1 nan-answer
                               :sum2 nan-answer
                               :sum3 nan-answer
                               :sum4 nan-answer}}
           (reductions/double-reductions {:sum1 :+
                                          :sum2 (:identity unop/builtin-ops)
                                          :sum3 (:+ binop/builtin-ops)
                                          :sum4 (reify DoubleReduction
                                                  (update [this lhs rhs]
                                                    (+ lhs rhs)))}
                                         nan-data
                                         {:nan-strategy-or-predicate :remove})))
    ))

(comment
  (do
    (def double-data (double-array (range 1000000)))
    (require '[criterium.core :as crit])
    (defn benchmark-indexed-reduction
      []
      (crit/quick-bench
       (-> (reductions/double-reducers->indexed-reduction
            {:sum1 :+
             :sum2 (:identity unop/builtin-ops)
             :sum3 (:+ binop/builtin-ops)
             :sum4 (reify DoubleReduction
                     (update [this lhs rhs]
                       (+ lhs rhs)))}
            :remove)
           (reductions/indexed-reduction double-data true))))
    ;;26.14ms
    (defn benchmark-pure-spliterator-reduction
      []
      (crit/quick-bench
       (-> (reductions/reader->double-spliterator double-data :remove)
           (parallel-for/spliterator-map-reduce
            (fn [^Spliterator$OfDouble dsplit]
              (.forEachRemaining dsplit
                                 (reify DoubleConsumer
                                   (accept [this data]
                                     (+ data 2.0))))
              2.0)
            (partial reduce +)))))

    (defn benchmark-serial-loop
      []
      (crit/quick-bench
       (let [rdr (dtype-base/->reader double-data)
             n-elems (.lsize rdr)
             consumer (reify DoubleConsumer
                        (accept [this data]
                          (+ data 2.0)))]
         (dotimes [idx n-elems]
           (.accept consumer (.readDouble rdr idx))))))

    (defn benchmark-parallel-loop
      []
      (let [rdr (dtype-base/->reader double-data)
            n-elems (.lsize rdr)
            consumer (DoubleConsumers$SummationConsumer.)
            consumer2 (reify DoubleConsumer
                        (accept [this data]))
            chained-consumer (.andThen consumer
                                       (reify DoubleConsumer
                                         (accept [this data])))
            c2-consumer (.andThen chained-consumer
                                  (reify DoubleConsumer
                                    (accept [this data])))]
        (parallel-for/indexed-map-reduce
         n-elems
         (fn [^long start-idx ^long group-len]
           (let [dconsumer (DoubleConsumers$SummationConsumer.)
                 nextconsumer (DoubleConsumers$SummationConsumer.)
                 combined (.andThen dconsumer nextconsumer)
                 predicate (reductions/nan-strategy-or-predicate->double-predicate
                            :remove)]
             (dotimes [idx group-len]
               (let [dval (.readDouble rdr (pmath/+ idx start-idx))]
                 (when (.test predicate dval)
                   (.accept combined dval))))
             (.result dconsumer)))
         (partial reduce (fn [^DoubleConsumers$DoubleConsumerResult res1
                              ^DoubleConsumers$DoubleConsumerResult res2]
                           (.combine res1 res2))))))

    )


  )
