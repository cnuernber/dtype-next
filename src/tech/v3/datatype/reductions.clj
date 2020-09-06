(ns tech.v3.datatype.reductions
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
            DoubleConsumers$StagedConsumer DoubleConsumers$Result
            DoubleConsumers DoubleConsumers$Sum DoubleConsumers$UnaryOpSum
            DoubleConsumers$BinaryOp
            DoubleConsumers$MultiStagedConsumer
            DoubleConsumers$MultiStagedResult]
           [java.util List Map HashMap Map$Entry Spliterator$OfDouble Spliterator]
           [java.util.concurrent ForkJoinPool Callable Future]
           [java.util.stream StreamSupport]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction BiConsumer DoublePredicate DoubleConsumer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def nan-strategies [:exception :keep :remove])
;;unspecified nan-strategy is :remove


(defn nan-strategy-or-predicate->double-predicate
  "Passing in either a keyword nan strategy #{:exception :keep :remove}
  or a DoublePredicate, return a DoublePredicate that filters
  double values."
  (^DoublePredicate [nan-strategy-or-predicate]
   (cond
     (instance? DoublePredicate nan-strategy-or-predicate)
     nan-strategy-or-predicate
     (= nan-strategy-or-predicate :keep)
     nil
     ;;Remove is the default so nil maps to remove
     (or (nil? nan-strategy-or-predicate)
         (= nan-strategy-or-predicate :remove))
     PrimitiveIODoubleSpliterator/removePredicate
     (= nan-strategy-or-predicate :exception)
     PrimitiveIODoubleSpliterator/exceptPredicate
     :else
     (errors/throwf "Unrecognized predicate: %s" nan-strategy-or-predicate)))
  ;;Remember the default predicate is :remove
  (^DoublePredicate []
   (nan-strategy-or-predicate->double-predicate nil)))


(defn double-summation
  "The most common operation gets its own pathway"
  ^double [rdr]
  (let [rdr (dtype-base/->reader rdr)
        n-elems (.size rdr)]
    (double
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long group-len]
        (let [end-idx (+ start-idx group-len)]
          (loop [idx (inc start-idx)
                 accum (.readDouble rdr start-idx)]
            (if (< idx end-idx)
              (recur (unchecked-inc idx)
                     (pmath/+ accum (.readDouble rdr idx)))
              accum))))
      (partial reduce +)))))


(defn staged-double-consumer-reduction
  (^DoubleConsumers$Result [staged-consumer-fn {:keys [nan-strategy-or-predicate]} rdr]
   (let [rdr (dtype-base/->reader rdr)
         n-elems (.size rdr)
         reduce-fn #(reduce (fn [^DoubleConsumers$Result lhs
                                 ^DoubleConsumers$Result rhs]
                              (.combine lhs rhs))
                            %)
         predicate (nan-strategy-or-predicate->double-predicate
                    nan-strategy-or-predicate)]
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long group-len]
        (DoubleConsumers/consume start-idx (int group-len) rdr
                                 (staged-consumer-fn) predicate))
      reduce-fn)))
  (^DoubleConsumers$Result [staged-consumer-fn rdr]
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


(defn staged-double-reductions
  ([reducer-map options rdr]
   (let [reducer-names (vec (keys reducer-map))
         consumer-fns (mapv reducer-value->consumer-fn (vals reducer-map))
         n-reducers (count reducer-names)]
     (cond
       (== 0 n-reducers)
       nil
       (== 1 n-reducers)
       (let [result (staged-double-consumer-reduction (first consumer-fns) options rdr)]
         {:n-elems (.nElems result)
          :data {(first reducer-names) (.value result)}})
       :else
       (let [consumer-fn (fn []
                           (DoubleConsumers$MultiStagedConsumer.
                            ^"L[tech.v3.datatype.DoubleConsumers$StagedConsumer;"
                            (into-array (Class/forName "tech.v3.datatype.DoubleConsumers$StagedConsumer")
                                        (map #(%) consumer-fns))))
             ^DoubleConsumers$MultiStagedResult result
             (staged-double-consumer-reduction consumer-fn options rdr)
             result-ary (.results result)]
         {:n-elems (.nElems ^DoubleConsumers$Result (aget result-ary 0))
          :data (->> (map (fn [n ^DoubleConsumers$Result r]
                            [n (.value r)])
                          reducer-names result-ary)
                     (into {}))}))))
  ([reducer-map rdr]
   (staged-double-reductions reducer-map {} rdr)))


(defn unary-double-summation
  ^double [^UnaryOperator op rdr]
  (let [rdr (dtype-base/->reader rdr)
        n-elems (.size rdr)]
    (double
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long group-len]
        (let [end-idx (+ start-idx group-len)]
          (loop [idx (inc start-idx)
                 accum (.readDouble rdr start-idx)]
            (if (< idx end-idx)
              (recur (unchecked-inc idx)
                     (pmath/+ accum
                              (.unaryDouble op (.readDouble rdr idx))))
              accum))))
      (partial reduce +)))))


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


(defn reader->double-spliterator
  "Convert a primitiveIO object into a spliterator with an optional
  nan strategy [:keep :remove :exception] or an implementation of a
  java.util.functions.DoublePredicate that can filter the double stream
  (or throw an exception on an invalid value).
  Implementations of UnaryPredicate also implement DoublePredicate."
  (^Spliterator$OfDouble [rdr nan-strategy-or-predicate]
   (if-let [rdr (dtype-base/->reader rdr)]
     (PrimitiveIODoubleSpliterator. rdr 0
                                    (.lsize rdr)
                                    (nan-strategy-or-predicate->double-predicate
                                     nan-strategy-or-predicate))
     (errors/throwf "Argument %s is not convertible to reader" (type rdr))))
  (^Spliterator$OfDouble [rdr]
   (reader->double-spliterator rdr nil)))


(deftype RetainDoubleConsumer [^:unsynchronized-mutable ^double value]
  DoubleConsumer
  (accept [this data]
    (set! value data))
  clojure.lang.IFn
  (invoke [this] value))


(defrecord SpliteratorReductionResult [^double data ^long n-elems])


(defn spliterator-double-unary-summation
  ^SpliteratorReductionResult [^UnaryOperator op ^Spliterator$OfDouble data]
  (parallel-for/spliterator-map-reduce
   data
   (fn [^Spliterator$OfDouble spliterator]
     (let [retain-consumer (RetainDoubleConsumer. Double/NaN)]
       (if (.tryAdvance spliterator retain-consumer)
         (let [result (double-array [(double (retain-consumer))])
               n-elems (long-array [1])]
           (.forEachRemaining spliterator
                              (reify DoubleConsumer
                                (accept [this value]
                                  (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
                                  (aset result 0
                                        (pmath/+ (aget result 0)
                                                 (.unaryDouble op value))))))
           (->SpliteratorReductionResult (aget result 0) (aget n-elems 0))))))
   (partial reduce (fn [^SpliteratorReductionResult lhs
                        ^SpliteratorReductionResult rhs]
                     (->SpliteratorReductionResult (pmath/+ (.data lhs) (.data rhs))
                                                   (pmath/+ (.n-elems lhs) (.n-elems rhs)))))))


(defn spliterator-double-binary-reduction
  (^SpliteratorReductionResult [^BinaryOperator op ^Spliterator$OfDouble data]
   (parallel-for/spliterator-map-reduce
    data
    (fn [^Spliterator$OfDouble spliterator]
      (let [retain-consumer (RetainDoubleConsumer. Double/NaN)]
        (if (.tryAdvance spliterator retain-consumer)
          (let [result (double-array [(double (retain-consumer))])
                n-elems (long-array [1])]
            (.forEachRemaining spliterator
                               (reify DoubleConsumer
                                 (accept [this value]
                                   (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
                                   (aset result 0
                                         (.binaryDouble op (aget result 0) value)))))
            (->SpliteratorReductionResult (aget result 0) (aget n-elems 0))))))
    (partial reduce (fn [^SpliteratorReductionResult lhs
                         ^SpliteratorReductionResult rhs]
                      (->SpliteratorReductionResult (.binaryDouble op (.data lhs) (.data rhs))
                                                    (pmath/+ (.n-elems lhs) (.n-elems rhs))))))))


(defn spliterator-double-reduction
  (^SpliteratorReductionResult [^DoubleReduction op ^Spliterator$OfDouble data
                                {:keys [finalize?]
                                 :or {finalize? true}}]
   (let [^SpliteratorReductionResult retval
         (parallel-for/spliterator-map-reduce
          data
          (fn [^Spliterator$OfDouble spliterator]
            (let [retain-consumer (RetainDoubleConsumer. Double/NaN)]
              (if (.tryAdvance spliterator retain-consumer)
                (let [result (double-array [(.elemwise op (double (retain-consumer)))])
                      n-elems (long-array [1])]
                  (.forEachRemaining spliterator
                                     (reify DoubleConsumer
                                       (accept [this value]
                                         (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
                                         (aset result 0
                                               (.update op (aget result 0)
                                                        (.elemwise op value))))))
                  (->SpliteratorReductionResult (aget result 0) (aget n-elems 0))))))
          (partial reduce (fn [^SpliteratorReductionResult lhs
                               ^SpliteratorReductionResult rhs]
                            (->SpliteratorReductionResult (.merge op (.data lhs) (.data rhs))
                                                          (pmath/+ (.n-elems lhs) (.n-elems rhs))))))]
     (if finalize?
       (->SpliteratorReductionResult (.finalize op (.data retval) (.n-elems retval))
                                     (.n-elems retval))
       retval)))
  (^SpliteratorReductionResult [^DoubleReduction op ^Spliterator$OfDouble data]
   (spliterator-double-reduction op data nil)))



(defn indexed-reduction
  ([^IndexReduction reducer rdr finalize?]
   (let [rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)
         batch-data (.prepareBatch reducer rdr)
         retval
         (parallel-for/indexed-map-reduce
          n-elems
          (fn [^long start-idx ^long group-len]
            (let [end-idx (+ start-idx group-len)]
              (loop [idx start-idx
                     ctx nil]
                (if (< idx end-idx)
                  (recur (unchecked-inc idx)
                         (.reduceIndex reducer batch-data ctx idx))
                  ctx))))
          (partial reduce (fn [lhs-ctx rhs-ctx]
                            (.reduceReductions reducer lhs-ctx rhs-ctx))))]
     (if finalize?
       (.finalize reducer retval)
       retval)))
  ([^IndexReduction reducer rdr]
   (indexed-reduction reducer rdr true)))


(defn ensure-double-reduction
  ^DoubleReduction [arg]
  (cond
    (instance? DoubleReduction arg)
    arg
    (= arg :+)
    (reify DoubleReduction
      (update [this lhs rhs]
        (pmath/+ lhs rhs))
      (merge [this lhs rhs]
        (pmath/+ lhs rhs)))
    (instance? UnaryOperator arg)
    (let [^UnaryOperator arg arg]
      (reify DoubleReduction
        (elemwise [this item]
          (.unaryDouble arg item))))
    (instance? BinaryOperator arg)
    (let [^BinaryOperator arg arg]
      (reify DoubleReduction
        (update [this lhs rhs]
          (.binaryDouble arg lhs rhs))
        (merge [this lhs rhs]
          (.binaryDouble arg lhs rhs))))
    :else
    (throw (Exception. "Argument not convertible to a double reduction: %s"
                       (type arg)))))


(defrecord DoubleReductionContext [^doubles data ^longs n-elem-ary])


;;Pass potentially many reducers in parallel over a reader
(deftype IndexedDoubleReduction [^objects reducers ^long n-reducers
                                 reducer-names ^DoublePredicate predicate]
  IndexReduction
  (reduceIndex [this batch-data ctx idx]
    (let [dval (.readDouble ^PrimitiveIO batch-data idx)
          valid? (if predicate
                   (.test predicate dval)
                   true)]
      ;;No predicate means use everything
      (if valid?
        (let [first? (nil? ctx)
              ^DoubleReductionContext ctx (or ctx
                                              (->DoubleReductionContext
                                               (double-array n-reducers)
                                               (long-array 1)))
              ^doubles data (.data ctx)
              ^longs n-elems (.n-elem-ary ctx)]
          (if first?
            (do
              (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
              (dotimes [reducer-idx n-reducers]
                (aset data reducer-idx
                      (.elemwise ^DoubleReduction (aget reducers reducer-idx)
                                 dval))))
            (do
              (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
              (dotimes [reducer-idx n-reducers]
                (aset data reducer-idx
                      (.update
                       ^DoubleReduction (aget reducers reducer-idx)
                       (aget data reducer-idx)
                       (.elemwise ^DoubleReduction (aget reducers reducer-idx)
                                  dval))))))
          ctx)
        ctx)))
  (reduceReductions [this lhs-ctx rhs-ctx]
    (let [^DoubleReductionContext lhs-ctx lhs-ctx
          ^DoubleReductionContext rhs-ctx rhs-ctx]
      (dotimes [reducer-idx n-reducers]
        (aset ^doubles (.data lhs-ctx) reducer-idx
              (.merge ^DoubleReduction (aget reducers reducer-idx)
                      (aget ^doubles (.data lhs-ctx) reducer-idx)
                      (aget ^doubles (.data rhs-ctx) reducer-idx))))
      (aset ^longs (.n-elem-ary lhs-ctx) 0
            (pmath/+ (aget ^longs (.n-elem-ary lhs-ctx) 0)
                     (aget ^longs (.n-elem-ary rhs-ctx) 0)))
      lhs-ctx))
  (finalize [this ctx]
    (let [^DoubleReductionContext ctx ctx
          n-elems (aget ^longs (.n-elem-ary ctx) 0)
          data (.data ctx)]
      {:n-elems n-elems
       :data (->> (map (fn [k r v]
                         [k (.finalize ^DoubleReduction r v n-elems)])
                       reducer-names reducers data)
                  (into {}))})))


(defn double-reducers->indexed-reduction
  "Make an index reduction out of a map of reducer-name to reducer.  Stores intermediate values
  in double arrays.  Upon finalize, returns a map of reducer-name to finalized double reduction
  value."
  (^IndexReduction [reducer-map nan-strategy-or-predicate]
   (let [^List reducer-names (keys reducer-map)
         reducers (object-array (map ensure-double-reduction (vals reducer-map)))
         n-reducers (alength reducers)]
     (IndexedDoubleReduction. reducers n-reducers
                              reducer-names
                              (nan-strategy-or-predicate->double-predicate
                               nan-strategy-or-predicate))))
  (^IndexReduction [reducer-map]
   (double-reducers->indexed-reduction reducer-map nil)))


(defn- fast-double-reduction
  "Reduction fastpaths for single reductions"
  [reducer-name reducer rdr
   {:keys [nan-strategy-or-predicate] :as options}]
  (let [rdr (dtype-base/->reader rdr)
        n-elems (.lsize rdr)
        updater-fn (fn [^SpliteratorReductionResult reducer-result]
                     {:n-elems (.n-elems reducer-result)
                      :data {reducer-name (.data reducer-result)}})
        keep? (= :keep nan-strategy-or-predicate)]
       (cond
         (= :+ reducer)
         (if keep?
           {:n-elems n-elems
            :data {reducer-name (double-summation rdr)}}
           (-> (spliterator-double-unary-summation
                (:identity unop/builtin-ops)
                (reader->double-spliterator
                 rdr nan-strategy-or-predicate))
               (updater-fn)))
         (instance? UnaryOperator reducer)
         (if keep?
           {:n-elems n-elems
            :data {reducer-name (unary-double-summation reducer rdr)}}
           (-> (spliterator-double-unary-summation
                reducer
                (reader->double-spliterator
                 rdr nan-strategy-or-predicate))
               (updater-fn)))
         (instance? BinaryOperator reducer)
         (if keep?
           {:n-elems n-elems
            :data {reducer-name (commutative-binary-double reducer rdr)}}
           (-> (spliterator-double-binary-reduction
                reducer
                (reader->double-spliterator
                 rdr nan-strategy-or-predicate))
               (updater-fn)))
         (instance? DoubleReduction reducer)
         (-> (spliterator-double-reduction
              reducer
              (reader->double-spliterator
               rdr nan-strategy-or-predicate)
              options)
             (updater-fn)))))


(defn double-reductions
  "Given a map of name->reducer of DoubleReduction implementations and a rdr
  do an efficient two-level parallelized reduction and return the results in
  a map of name->finalized-result.
  Result shape
  {:n-elems - number of elements that passed the nan strategy or predicate
   :data - map of reducer-name to reduced double value.}"
  ([reducer-map rdr {:keys [nan-strategy-or-predicate finalize?]
                     :or {finalize? true}
                     :as options}]
   ;;It takes a lot of reducers in order to make it worth combining them
   (if (dtype-proto/convertible-to-reader? rdr)
     (if (< (count reducer-map) 5)
       (->> reducer-map
            (reduce
             (fn [result-map [reducer-name reducer]]
               (let [{:keys [n-elems data]}
                     (fast-double-reduction reducer-name reducer rdr options)]
                 (-> (assoc result-map :n-elems n-elems)
                     (update :data merge data))))
             {}))
       (-> (double-reducers->indexed-reduction
            reducer-map nan-strategy-or-predicate)
           (indexed-reduction rdr finalize?)))))
  ([reducer-map rdr]
   (double-reductions reducer-map rdr nil)))


(defn unordered-group-by-reduce
  "Perform an unordered group-by operation using reader and placing results
  into the result-map.  Expects that reducer's batch-data method has already been
  called and Returns the non-finalized result-map.
  If a map is passed in then it's compute operator needs to be threadsafe.
  If result-map is nil then one is created.
  This implementation takes advantage of the fact that for java8+, we have essentially a lock
  free concurrent hash map as long as there aren't collisions so it performs surprisingly well
  considering the amount of pressure this can put on the concurrency model."
  (^Map [^IndexReduction reducer batch-data rdr ^Map result-map]
   (let [^Map result-map (or result-map (ConcurrentHashMap.))
         bifn (IndexReduction$IndexedBiFunction. reducer batch-data)
         rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)]
     ;;Side effecting loop to compute values in-place
     (parallel-for/parallel-for
      idx
      n-elems
      (do
        ;;java.util.Map compute really should take more arguments but this way
        ;;the long is never boxed.
        (.setIndex bifn idx)
        (.compute result-map (.readObject rdr idx) bifn)))
     result-map))
  (^Map [reducer batch-data rdr]
   (unordered-group-by-reduce reducer batch-data rdr nil)))



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
   (let [bifn (IndexReduction$IndexedBiFunction. reducer batch-data)
         rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)
         merge-bifn (reify BiFunction
                      (apply [this lhs rhs]
                        (.reduceReductions reducer lhs rhs)))]
     ;;Side effecting loop to compute values in-place
     (parallel-for/indexed-map-reduce
      n-elems
      (fn [^long start-idx ^long group-len]
        (let [result-map (HashMap.)
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
                        lhs-map))))))


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
