(ns tech.v3.datatype.reductions
  "High performance reductions based on tech.v3.datatype concepts as well
  as java stream concepts."
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.unary-op :as unop]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryOperator IndexReduction DoubleReduction
            PrimitiveIO IndexReduction$IndexedBiFunction UnaryOperator
            PrimitiveIOIterator PrimitiveIODoubleSpliterator
            Consumers$StagedConsumer Consumers$Result
            DoubleConsumers DoubleConsumers$Sum DoubleConsumers$UnaryOpSum
            DoubleConsumers$BinaryOp
            Consumers$MultiStagedConsumer
            Consumers$MultiStagedResult]
           [java.util List Map HashMap Map$Entry Spliterator$OfDouble Spliterator]
           [java.util.concurrent ForkJoinPool Callable Future]
           [java.util.stream StreamSupport]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction BiConsumer DoublePredicate DoubleConsumer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def nan-strategies [:exception :keep :remove])
;;unspecified nan-strategy is :remove


(defn nan-strategy->double-predicate
  "Passing in either a keyword nan strategy #{:exception :keep :remove}
  or a DoublePredicate, return a DoublePredicate that filters
  double values."
  (^DoublePredicate [nan-strategy]
   (cond
     (instance? DoublePredicate nan-strategy)
     nan-strategy
     (= nan-strategy :keep)
     nil
     ;;Remove is the default so nil maps to remove
     (or (nil? nan-strategy)
         (= nan-strategy :remove))
     PrimitiveIODoubleSpliterator/removePredicate
     (= nan-strategy :exception)
     PrimitiveIODoubleSpliterator/exceptPredicate
     :else
     (errors/throwf "Unrecognized predicate: %s" nan-strategy)))
  ;;Remember the default predicate is :remove
  (^DoublePredicate []
   (nan-strategy->double-predicate nil)))


(defn- reduce-consumer-results
  [consumer-results]
  (reduce (fn [^Consumers$Result lhs
               ^Consumers$Result rhs]
            (.combine lhs rhs))
          consumer-results))


(defn staged-double-consumer-reduction
  (^Consumers$Result [staged-consumer-fn {:keys [nan-strategy]} rdr]
   (let [rdr (dtype-base/->reader rdr)
         n-elems (.size rdr)
         predicate (nan-strategy->double-predicate
                    nan-strategy)]
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long group-len]
        (DoubleConsumers/consume start-idx (int group-len) rdr
                                 (staged-consumer-fn) predicate))
      reduce-consumer-results)))
  (^Consumers$Result [staged-consumer-fn rdr]
   (staged-double-consumer-reduction staged-consumer-fn nil rdr)))


(defn reducer-value->consumer-fn
  [reducer-value]
  (cond
    ;;Hopefully this returns what we think it should...
    (fn? reducer-value)
    reducer-value
    (= :+ reducer-value)
    #(DoubleConsumers$Sum.)
    (instance? UnaryOperator reducer-value)
    #(DoubleConsumers$UnaryOpSum. reducer-value)
    (instance? BinaryOperator reducer-value)
    (let [^BinaryOperator op reducer-value]
      #(DoubleConsumers$BinaryOp. op (.initialDoubleReductionValue op)))
    :else
    (errors/throwf "Connot convert value to double consumer: %s" reducer-value)))


(defn double-reductions
  ([reducer-map options rdr]
   (let [n-reducers (count reducer-map)]
     (cond
       (== 0 n-reducers)
       nil
       (or (:serial-reduction? options)
           (== 1 n-reducers))
       (reduce (fn [accum [reducer-name reducer]]
                 (let [result (staged-double-consumer-reduction
                               (reducer-value->consumer-fn reducer)
                               options rdr)]
                   (-> (assoc accum :n-elems (.nElems result))
                       (update :data merge {reducer-name (.value result)}))))
               {}
               reducer-map)
       :else
       (let [reducer-names (vec (keys reducer-map))
             consumer-fns (mapv reducer-value->consumer-fn
                                (vals reducer-map))
             consumer-fn (fn []
                           (Consumers$MultiStagedConsumer.
                            ^"L[tech.v3.datatype.Consumers$StagedConsumer;"
                            (into-array (Class/forName
                                         "tech.v3.datatype.Consumers$StagedConsumer")
                                        (map #(%) consumer-fns))))
             ^Consumers$MultiStagedResult result
             (staged-double-consumer-reduction consumer-fn options rdr)
             result-ary (.results result)]
         {:n-elems (.nElems ^Consumers$Result (aget result-ary 0))
          :data (->> (map (fn [n ^Consumers$Result r]
                            [n (.value r)])
                          reducer-names result-ary)
                     (into {}))}))))
  ([reducer-map rdr]
   (double-reductions reducer-map {} rdr)))


(defn double-summation
  "The most common operation gets its own pathway"
  (^double [options rdr]
   (double (.value (staged-double-consumer-reduction
                    (reducer-value->consumer-fn :+)
                    options
                    rdr))))
  ([rdr]
   (double-summation {} rdr)))


(defn unary-double-summation
  (^double [^UnaryOperator op options rdr]
   (double (.value (staged-double-consumer-reduction
                    (reducer-value->consumer-fn op)
                    options
                    rdr))))
  (^double [op rdr]
   (unary-double-summation op {} rdr)))


(defn commutative-binary-double
  (^double [^BinaryOperator op options rdr]
   (double (.value (staged-double-consumer-reduction
                    (reducer-value->consumer-fn op)
                    options
                    rdr))))
  (^double [op rdr]
   (commutative-binary-double op {} rdr)))


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
  (if-let [rdr (dtype-base/as-reader rdr)]
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


(defn reader->double-spliterator
  "Convert a primitiveIO object into a spliterator with an optional
  nan strategy [:keep :remove :exception] or an implementation of a
  java.util.functions.DoublePredicate that can filter the double stream
  (or throw an exception on an invalid value).
  Implementations of UnaryPredicate also implement DoublePredicate."
  (^Spliterator$OfDouble [rdr nan-strategy]
   (if-let [rdr (dtype-base/->reader rdr)]
     (PrimitiveIODoubleSpliterator. rdr 0
                                    (.lsize rdr)
                                    (nan-strategy->double-predicate nan-strategy))
     (errors/throwf "Argument %s is not convertible to reader" (type rdr))))
  (^Spliterator$OfDouble [rdr]
   (reader->double-spliterator rdr nil)))


(defn unordered-group-by-reduce
  "Perform an unordered group-by operation using reader and placing results
  into the result-map.  Expects that reducer's batch-data method has already been
  called and Returns the non-finalized result-map.
  If a map is passed in then it's compute operator needs to be threadsafe.
  If result-map is nil then one is created.
  This implementation takes advantage of the fact that for java8+ we have essentially a lock
  free concurrent hash map as long as there aren't collisions so it performs surprisingly well
  considering the amount of pressure this can put on the concurrency model.
  Unordered has an advantage with very large datasets in that it does not require a merge step
  at the end of the parallelized group-by pass."
  (^Map [^IndexReduction reducer batch-data rdr ^Map result-map]
   (let [^Map result-map (or result-map (ConcurrentHashMap.))
         rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)]
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long group-len]
        (let [bifn (IndexReduction$IndexedBiFunction. reducer batch-data)
              end-idx (+ start-idx group-len)]
          (loop [idx start-idx]
            (when (< idx end-idx)
              (.setIndex bifn idx)
              (.compute result-map (.readObject rdr idx) bifn)
              (recur (unchecked-inc idx))))
          result-map)))
     result-map))
  (^Map [reducer batch-data rdr]
   (unordered-group-by-reduce reducer batch-data rdr nil))
  (^Map [reducer rdr]
   (unordered-group-by-reduce reducer nil rdr nil)))



(defn ordered-group-by-reduce
  "Perform an ordered group-by operation using reader and placing results
  into the result-map.  Expects that reducer's batch-data method has already been
  called and Returns the non-finalized result-map.
  If result-map is nil then one is created.
  Each bucket's results end up ordered by index iteration order.  The original parallel pass
  goes through each index in order and then the reduction goes through the thread groups in order
  so if your index reduction merger just does (.addAll lhs rhs) then the final result ends up
  ordered."
  (^Map [^IndexReduction reducer batch-data rdr]
   (let [rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)
         merge-bifn (reify BiFunction
                      (apply [this lhs rhs]
                        (.reduceReductions reducer lhs rhs)))]
     ;;Side effecting loop to compute values in-place
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long group-len]
        (let [result-map (HashMap.)
              bifn (IndexReduction$IndexedBiFunction. reducer batch-data)
              end-idx (+ start-idx group-len)]
          (loop [idx start-idx]
            (when (< idx end-idx)
              (.setIndex bifn idx)
              (.compute result-map (.readObject rdr idx) bifn)
              (recur (unchecked-inc idx))))
          result-map))
      (partial reduce (fn [^Map lhs-map ^Map rhs-map]
                (.forEach rhs-map
                          (reify BiConsumer
                            (accept [this k v]
                              (.merge lhs-map k v merge-bifn))))
                lhs-map)))))
  (^Map [^IndexReduction reducer rdr]
   (ordered-group-by-reduce reducer nil rdr)))


(comment
  ;;Testing different iteration strategies
  (def double-data (double-array (range 1000000)))
  (defn parallel-iteration-time-test
    []
    (parallel-for/indexed-map-reduce
     (alength double-data)
     (fn [^long start-idx ^long group-len]
       (let [rdr (-> (dtype-base/sub-buffer double-data start-idx group-len)
                     (dtype-base/->reader))
             ^PrimitiveIOIterator iterator (.iterator rdr)]
         (loop [continue? (.hasNext iterator)
                first? true
                accum Double/NaN]
           (if continue?
             (let [value (.nextDouble iterator)]
               (recur (.hasNext iterator) false
                      (if first? value (+ accum value))))
             accum))))
     (partial reduce +)))

  )
