(ns tech.v3.datatype.reductions
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype BinaryOperator IndexReduction DoubleReduction
            PrimitiveIO IndexReduction$IndexedBiFunction UnaryOperator
            PrimitiveIOIterator]
           [org.apache.commons.math3.exception NotANumberException]
           [java.util List Map HashMap Map$Entry]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction BiConsumer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def nan-strategies [:exception :keep :ignore])


(defmacro check-nan-exception
  [arg]
  `(if (Double/isNaN (arg))
     (throw (NotANumberException. "NaN detected"))
     arg))

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
  ([op rdr nan-strategy]
   (if-let [rdr (dtype-base/->reader rdr)]
     (if (instance? BinaryOperator op)
       (let [rdr-dtype (dtype-base/elemwise-datatype rdr)]
         (cond
           (casting/integer-type? rdr-dtype)
           (commutative-binary-long op rdr)
           (casting/float-type? rdr-dtype)
           (commutative-binary-double op rdr nan-strategy)
           :else
           (commutative-binary-object op rdr)))
       (commutative-binary-object op rdr))
     ;;Clojure core reduce is actually pretty good!
     (reduce op rdr)))
  ([op rdr] (commutative-binary-reduce op rdr :keep)))


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


(deftype KeepIndexedDoubleReduction [^objects reducers ^long n-reducers
                                     reducer-names nan-strategy]
  IndexReduction
  (reduceIndex [this batch-data ctx idx]
    (let [dval (.readDouble ^PrimitiveIO batch-data idx)]
      (if (or (= nan-strategy :keep)
              (not (Double/isNaN dval)))
        (if-not ctx
          (let [^DoubleReductionContext ctx (->DoubleReductionContext
                                             (double-array n-reducers)
                                             (long-array 1))
                ^doubles data (.data ctx)
                ^longs n-elems (.n-elem-ary ctx)]
            (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
            (dotimes [reducer-idx n-reducers]
              (aset data reducer-idx
                    (.elemwise ^DoubleReduction (aget reducers reducer-idx)
                               dval)))
            ctx)
          (let [^DoubleReductionContext ctx ctx
                ^doubles data (.data ctx)
                ^longs n-elems (.n-elem-ary ctx)]
            (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
            (dotimes [reducer-idx n-reducers]
              (aset data reducer-idx
                    (.update
                     ^DoubleReduction (aget reducers reducer-idx)
                     (aget data reducer-idx)
                     (.elemwise ^DoubleReduction (aget reducers reducer-idx)
                                dval))))
            ctx)))))
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
      (->> (map (fn [k r v]
                  [k (.finalize ^DoubleReduction r v n-elems)])
                reducer-names reducers data)
           (into {})))))

(deftype NanStrategyIndexedDoubleReduction
    [^objects reducers ^long n-reducers
     reducer-names nan-strategy]
  IndexReduction
  (reduceIndex [this batch-data ctx idx]
    (let [dval (.readDouble ^PrimitiveIO batch-data idx)]
      (if-not (Double/isNaN dval)
        (if-not ctx
          (let [^DoubleReductionContext ctx (->DoubleReductionContext
                                             (double-array n-reducers)
                                             (long-array 1))
                ^doubles data (.data ctx)
                ^longs n-elems (.n-elem-ary ctx)]
            (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
            (dotimes [reducer-idx n-reducers]
              (aset data reducer-idx
                    (.elemwise ^DoubleReduction (aget reducers reducer-idx)
                               dval)))
            ctx)
          (let [^DoubleReductionContext ctx ctx
                ^doubles data (.data ctx)
                ^longs n-elems (.n-elem-ary ctx)]
            (aset n-elems 0 (unchecked-inc (aget n-elems 0)))
            (dotimes [reducer-idx n-reducers]
              (aset data reducer-idx
                    (.update
                     ^DoubleReduction (aget reducers reducer-idx)
                     (aget data reducer-idx)
                     (.elemwise ^DoubleReduction (aget reducers reducer-idx)
                                dval))))
            ctx)))))
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
      (->> (map (fn [k r v]
                  [k (.finalize ^DoubleReduction r v n-elems)])
                reducer-names reducers data)
           (into {})))))


(defn double-reducers->indexed-reduction
  "Make an index reduction out of a map of reducer-name to reducer.  Stores intermediate values
  in double arrays.  Upon finalize, returns a map of reducer-name to finalized double reduction
  value."
  ^IndexReduction [reducer-map]
  (let [^List reducer-names (keys reducer-map)
        reducers (object-array (map ensure-double-reduction (vals reducer-map)))
        n-reducers (alength reducers)]
))


(defn double-reductions
  "Given a map of name->reducer of DoubleReduction implementations and a rdr
  do an efficient two-level parallelized reduction and return the results in
  a map of name->finalized-result."
  ([reducer-map rdr finalize?]
   (if (== 1 (count reducer-map))
     (let [[reducer-name reducer] (first reducer-map)]
       (if (instance? UnaryOperator reducer)
         {reducer-name (unary-summation reducer rdr)}
         (let [rdr (dtype-base/->reader rdr)
               ^DoubleReduction reducer reducer
               n-elems (.lsize rdr)
               retval
               (parallel-for/indexed-map-reduce
                n-elems
                (fn [^long start-idx ^long group-len]
                  (let [end-idx (pmath/+ start-idx group-len)]
                    (loop [idx (unchecked-inc start-idx)
                           accum (.elemwise reducer (.readDouble rdr idx))]
                      (if (< idx end-idx)
                        (recur (unchecked-inc idx)
                               (.update reducer accum
                                        (.elemwise reducer
                                                   (.readDouble rdr idx))))
                        accum))))
                (partial reduce #(.merge reducer (double %1) (double %2))))]
           (if finalize?
             {reducer-name (.finalize reducer (double retval) (double n-elems))}
             {reducer-name retval}))))
     (-> (double-reducers->indexed-reduction reducer-map)
         (indexed-reduction rdr finalize?))))
  ([reducer-map rdr]
   (double-reductions reducer-map rdr true)))


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
