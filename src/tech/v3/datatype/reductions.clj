(ns tech.v3.datatype.reductions
  "High performance reductions based on tech.v3.datatype concepts as well
  as java stream concepts."
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf])
  (:import [tech.v3.datatype BinaryOperator IndexReduction
            IndexReduction$IndexedBiFunction UnaryOperator
            BufferIterator
            MultiConsumer Buffer]
           [ham_fisted Reducible Reductions Sum IFnDef$OLO IFnDef$ODO]
           [clojure.lang IDeref]
           [java.util Map Spliterator$OfDouble LinkedHashMap]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction BiConsumer DoublePredicate DoubleConsumer
            LongConsumer Consumer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def nan-strategies [:exception :keep :remove])
;;unspecified nan-strategy is :remove


(defn- reduce-consumer-results
  [consumer-results]
  (Reductions/reduceReducibles consumer-results))


(deftype UnOpSum [^Sum value
                  ^UnaryOperator unop]
  DoubleConsumer
  (accept [this v] (.accept value (.unaryDouble unop v)))
  Reducible
  (reduce [this v] (.reduce value v))
  IDeref
  (deref [this] {:value (.deref value)}))


(deftype BinDoubleConsumer [^{:unsynchronized-mutable true
                              :tag 'double} value
                            ^BinaryOperator binop]
  DoubleConsumer
  (accept [this lval]
    (set! value (.binaryDouble binop value lval)))
  Reducible
  (reduce [this rhs]
    (set! value (.binaryDouble binop value (double @rhs)))
    this)
  IDeref
  (deref [this] {:value value}))


(deftype BinLongConsumer [^{:unsynchronized-mutable true
                            :tag 'long} value
                          ^{:unsynchronized-mutable true
                            :tag 'boolean} first
                          ^BinaryOperator binop]
  LongConsumer
  (accept [this lval]
    (if first
      (do
        (set! first false)
        (set! value lval))
      (set! value (.binaryLong binop value lval))))
  Reducible
  (reduce [this rhs]
    (set! value (.binaryLong binop value (long @rhs)))
    this)
  IDeref
  (deref [this] value))


(defn reducer-value->consumer-fn
  "Produce a consumer from a generic reducer value."
  [reducer-value]
  (cond
    ;;Hopefully this returns what we think it should...
    (fn? reducer-value)
    reducer-value
    (= :tech.numerics/+ reducer-value)
    #(Sum.)
    (instance? UnaryOperator reducer-value)
    #(UnOpSum. (Sum.) reducer-value)
    (instance? BinaryOperator reducer-value)
    #(BinDoubleConsumer. (.initialDoubleReductionValue ^BinaryOperator reducer-value)
                         reducer-value)
    :else
    (errors/throwf "Connot convert value to double consumer: %s" reducer-value)))


(defn staged-double-consumer-reduction
  "Given a function that produces an implementation of
  tech.v3.datatype.Consumers$StagedConsumer perform a reduction.

  A staged consumer is a consumer can be used in a map-reduce pathway
  where during the map portion .consume is called and then during produces
  a 'result' on which .combine is called during the reduce pathway.

  See options for ham-fisted/preduce."
  ([staged-consumer-fn options rdr]
   (let [rdr (or (dtype-base/as-reader rdr :float64) rdr)
         rdr (case (get options :nan-strategy :remove)
               :remove (lznc/filter (hamf/double-predicate v (not (Double/isNaN v))) rdr)
               :keep rdr
               :exception (lznc/map (hamf/double-unary-operator
                                     v
                                     (when (Double/isNaN v)
                                       (throw (RuntimeException. "NaN detected")))
                                     v)))
         staged-consumer-fn (reducer-value->consumer-fn staged-consumer-fn)]
     (-> (hamf/preduce staged-consumer-fn hamf/double-consumer-accumulator
                       hamf/reducible-merge
                       options
                       rdr)
         (deref))))
  ([staged-consumer-fn rdr]
   (staged-double-consumer-reduction staged-consumer-fn nil rdr)))


(defn double-reductions
  "Perform a group of reductions on a single double reader."
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
                   (-> (assoc accum :n-elems (:n-elems result))
                       (update :data merge {reducer-name result}))))
               {}
               reducer-map)
       :else
       (let [reducer-names (vec (keys reducer-map))
             consumer-fns (mapv reducer-value->consumer-fn
                                (vals reducer-map))
             consumer-fn (fn []
                           (MultiConsumer.
                            (into-array (Class/forName
                                         "ham_fisted.Reducible")
                                        consumer-fns)))
             ^objects result-ary
             (staged-double-consumer-reduction consumer-fn options rdr)]
         {:n-elems (:n-elems (aget result-ary 0))
          :data (->> (map vector
                          reducer-names result-ary)
                     (into {}))}))))
  ([reducer-map rdr]
   (double-reductions reducer-map {} rdr)))


(defn double-summation
  "Double sum of data using
  [Kahan compensated summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm)."
  (^double [options rdr]
   (double (:sum (staged-double-consumer-reduction
                  (reducer-value->consumer-fn :tech.numerics/+)
                  options
                  rdr))))
  (^double [rdr]
   (double-summation {} rdr)))


(defn unary-double-summation
  "Perform a double summation using a unary operator to transform the input stream
  into a new double stream."
  (^double [^UnaryOperator op options rdr]
   (double (:value (staged-double-consumer-reduction
                    (reducer-value->consumer-fn op)
                    options
                    rdr))))
  (^double [op rdr]
   (unary-double-summation op {} rdr)))


(defn commutative-binary-double
  "Perform a commutative reduction using a binary operator to perform
  the reduction.  The operator needs to be both commutative and associative."
  (^double [^BinaryOperator op options rdr]
   (double (:value (staged-double-consumer-reduction
                    (reducer-value->consumer-fn op)
                    options
                    rdr))))
  (^double [op rdr]
   (commutative-binary-double op {} rdr)))

(deftype BinLongConsumer [^{:unsynchronized-mutable true
                            :tag 'long} value
                          ^{:unsynchronized-mutable true
                            :tag 'boolean} first
                          ^BinaryOperator binop]
  LongConsumer
  (accept [this lval]
    (if first
      (do
        (set! first false)
        (set! value lval))
      (set! value (.binaryLong binop value lval))))
  Reducible
  (reduce [this rhs]
    (set! value (.binaryLong binop value (long @rhs)))
    this)
  IDeref
  (deref [this] value))


(defn commutative-binary-long
  "Perform a commutative reduction in int64 space using a binary operator.  The
  operator needs to be both commutative and associative."
  ^long [^BinaryOperator op rdr]
  (let [rdr (or (dtype-base/as-reader rdr :int64) rdr)]
    @(hamf/preduce #(BinLongConsumer. 0 true op)
                   hamf/long-consumer-accumulator
                   (fn [^Reducible lhs rhs]
                     (.reduce lhs rhs))
                   {:ordered? true
                    :min-n 10000}
                   rdr)))


(defn commutative-binary-reduce
  [^BinaryOperator op data]
  (let [op-dtype (casting/simple-operation-space (dtype-base/elemwise-datatype data)
                                                 (get (meta op) :operational-space :object))

        data (or (dtype-base/as-reader data op-dtype) data)
        ival (hamf/first data)
        data (if (instance? Buffer data)
               (if (pos? (.lsize ^Buffer data))
                 (.subBuffer ^Buffer data 1 (.lsize ^Buffer data))
                 [])
               (rest data))]
    (hamf/preduce (constantly ival)
                  (case op-dtype
                    :int64 (reify IFnDef$OLO
                             (invokePrim [this acc l]
                               (.binaryLong op (long acc) l)))
                    :float64 (reify IFnDef$ODO
                               (invokePrim [this acc l]
                                 (.binaryDouble op (double acc) l)))
                    op)
                  op
                  data)))


(deftype BinConsumer [^{:unsynchronized-mutable true
                        :tag 'long} value
                      ^{:unsynchronized-mutable true
                        :tag 'boolean} first
                      binop]
  Consumer
  (accept [this lval]
    (if first
      (do
        (set! first false)
        (set! value lval))
      (set! value (binop value lval))))
  Reducible
  (reduce [this rhs]
    (set! value (binop value @rhs))
    this)
  IDeref
  (deref [this] value))


(defn commutative-binary-object
  "Perform a commutative reductions in object space using a binary operator.  The
  operator needs to be both commutative and associative."
  [op rdr]
  (let [rdr (dtype-base/->reader rdr)]
        @(hamf/preduce #(BinConsumer. 0 true op)
                       (fn [^Consumer c v]
                         (.accept c v)
                         c)
                       (fn [^Reducible lhs rhs]
                         (.reduce lhs rhs))
                       {:ordered? true
                        :min-n 10000}
                       rdr)))


(defn unordered-group-by-reduce
  "Perform an unordered group-by operation using reader and placing results
  into the result-map.  Expects that reducer's batch-data method has already been
  called and returns the non-finalized result-map.

  * batch-data is the *result* of the reducer's prepareBatch function.


  If a map is passed in then it's compute operator needs to be threadsafe.
  If result-map is nil then one is created.
  This implementation takes advantage of the fact that for java8+ we have essentially a lock
  free concurrent hash map as long as there aren't collisions so it performs surprisingly well
  considering the amount of pressure this can put on the concurrency model.
  Unordered has an advantage with very large datasets in that it does not require a merge step
  at the end of the parallelized group-by pass.

  Returns the result map.  It is the caller's job to call the reducer's finalize on each
  map value if necessary."
  (^Map [^IndexReduction reducer batch-data rdr ^Map result-map]
   (let [^Map result-map (or result-map (ConcurrentHashMap.))
         rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)
         lp (.indexFilter reducer batch-data)]
     (->> (hamf/upgroups
           n-elems
           (fn [^long sidx ^long eidx]
             (let [bifn (IndexReduction$IndexedBiFunction. reducer batch-data)]
               (if lp
                 (loop [idx sidx]
                   (when (< idx eidx)
                     (when (.test lp idx)
                       (.setIndex bifn idx)
                       (.compute result-map (.readObject rdr idx) bifn))
                     (recur (unchecked-inc idx))))
                 (loop [idx sidx]
                   (when (< idx eidx)
                     (.setIndex bifn idx)
                     (.compute result-map (.readObject rdr idx) bifn)
                     (recur (unchecked-inc idx))))))))
          (dorun))
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
  ordered.

  This returns a java.util.LinkedHashMap.

Example:

```clojure
user> (import '[tech.v3.datatype IndexReduction])
tech.v3.datatype.IndexReduction
user> (require '[tech.v3.datatype :as dtype])
nil
user> (require '[tech.v3.datatype.reductions :as dt-reduce])
nil
user> (def key-col [:a :b :a :a :c :d])

#'user/key-col
user> (def val-col (mapv name key-col))
#'user/val-col
user> (dt-reduce/ordered-group-by-reduce
       (reify IndexReduction
         (reduceIndex [this batch ctx idx]
           (conj ctx (val-col idx)))
         (reduceReductions [this lhs rhs]
           (vec (concat lhs rhs))))
       key-col)
{:a (\"a\" \"a\" \"a\"), :b (\"b\"), :c (\"c\"), :d (\"d\")}
user>
```"
  (^Map [^IndexReduction reducer batch-data rdr]
   (let [rdr (dtype-base/->reader rdr)
         n-elems (.lsize rdr)
         merge-bifn (reify BiFunction
                      (apply [this lhs rhs]
                        (.reduceReductions reducer lhs rhs)))
         lp (.indexFilter reducer batch-data)]
     (->> (hamf/pgroups
           n-elems
           (fn [^long sidx ^long eidx]
             (let [result-map (LinkedHashMap.)
                   bifn (IndexReduction$IndexedBiFunction. reducer batch-data)]
               (if lp
                 (loop [idx sidx]
                   (when (< idx eidx)
                     (when (.test lp idx)
                       (.setIndex bifn idx)
                       (.compute result-map (.readObject rdr idx) bifn))
                     (recur (unchecked-inc idx))))
                 (loop [idx sidx]
                   (when (< idx eidx)
                     (.setIndex bifn idx)
                     (.compute result-map (.readObject rdr idx) bifn)
                     (recur (unchecked-inc idx)))))
               result-map)))
          (reduce (fn [^Map lhs-map ^Map rhs-map]
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
             ^BufferIterator iterator (.iterator rdr)]
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
