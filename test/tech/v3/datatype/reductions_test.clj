(ns tech.v3.datatype.reductions-test
  (:require [tech.v3.datatype.reductions :as reductions]
            [tech.v3.datatype.unary-op :as unop]
            [tech.v3.datatype.binary-op :as binop]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [primitive-math :as pmath]
            [clojure.test :refer [deftest is]])
  (:import [tech.v3.datatype DoubleReduction
            DoubleConsumers$Sum
            Consumers$Result
            DoubleConsumers$MinMaxSum
            DoubleConsumers$Moments]
           [java.util Spliterator Spliterator$OfDouble]
           [java.util.function DoubleConsumer]))


(deftest summation-reductions
  (let [data (double-array (range 100))
        nan-data (double-array (range 100))
        _ (aset nan-data 50 Double/NaN)
        answer (double (reduce + data))
        nan-answer (double (- answer 50))]
    (is (= {:n-elems 100 :data {:sum answer}}
           (reductions/double-reductions {:sum :+}
                                         {:nan-strategy :keep}
                                         data)))
    (is (= {:n-elems 99 :data {:sum nan-answer}}
           (reductions/double-reductions {:sum :+} nan-data)))
    (is (thrown? Throwable
                 (reductions/double-reductions
                  {:sum :+} nan-data
                  {:nan-strategy :exception})))
    (is (= {:n-elems 99 :data {:sum nan-answer}}
           (reductions/double-reductions {:sum (:identity unop/builtin-ops)}
                                         nan-data)))
    (is (= {:n-elems 99 :data {:sum nan-answer}}
           (reductions/double-reductions {:sum (:+ binop/builtin-ops)}
                                         nan-data)))

    (is (= {:n-elems 100 :data {:sum1 answer
                                :sum2 answer
                                :sum3 answer}}
           (reductions/double-reductions {:sum1 :+
                                          :sum2 (:identity unop/builtin-ops)
                                          :sum3 (:+ binop/builtin-ops)}
                                         {:nan-strategy :keep}
                                         data)))
    (is (= {:n-elems 99 :data {:sum1 nan-answer
                               :sum2 nan-answer
                               :sum3 nan-answer}}
           (reductions/double-reductions {:sum1 :+
                                          :sum2 (:identity unop/builtin-ops)
                                          :sum3 (:+ binop/builtin-ops)}
                                         {:nan-strategy :remove}
                                         nan-data)))
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
             :sum3 (:+ binop/builtin-ops)}
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

    (defn benchmark-sum-reduction
      []
      (crit/quick-bench (reductions/double-summation double-data)))


    (defn benchmark-double-consumer-reduction-keep
      []
      (crit/quick-bench
       (reductions/staged-double-consumer-reduction
        #(DoubleConsumers$Sum.) {:nan-strategy :keep}
        double-data)))


    (defn benchmark-double-consumer-reduction-remove
      []
      (crit/quick-bench
       (reductions/staged-double-consumer-reduction
        #(DoubleConsumers$Sum.) {:nan-strategy :remove}
        double-data)))


    (defn benchmark-multi-consumer-reduction
      []
      (crit/quick-bench
       (reductions/staged-double-reductions {:sum1 :+
                                             :min (:min binop/builtin-ops)
                                             :max (:max binop/builtin-ops)
                                             }
                                            {:serial-reduction? true}
                                            double-data)))

    (defn benchmark-min-max-sum-consumer
      []
      (crit/quick-bench
       (reductions/staged-double-consumer-reduction
        #(DoubleConsumers$MinMaxSum.) {:nan-strategy :remove}
        double-data)))


    (defn benchmark-moments-consumer
      []
      (crit/quick-bench
       (reductions/staged-double-consumer-reduction
        #(DoubleConsumers$Moments. 49000) {:nan-strategy :remove}
        double-data)))


    (import '[org.apache.commons.math3.stat.descriptive DescriptiveStatistics])

    (defn benchmark-desc-stats-sum-min-max
      []
      (crit/quick-bench
       (let [desc-stats (DescriptiveStatistics. double-data)]
         {:min (.getMin desc-stats)
          :max (.getMax desc-stats)
          :mean (.getMean desc-stats)})
       ))


    )


  )
