(ns tech.v3.datatype.reductions
  "High performance reductions based on tech.v3.datatype concepts as well
  as java stream concepts."
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf])
  (:import [tech.v3.datatype BinaryOperator UnaryOperator Buffer]
           [ham_fisted Reducible Reductions Sum IFnDef$OLO IFnDef$ODO Casts]
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
  (deref [this] (.deref value)))


(deftype BinDoubleConsumer [^{:unsynchronized-mutable true
                              :tag double} value
                            ^{:unsynchronized-mutable true
                              :tag long} n-elems
                            ^BinaryOperator binop]
  DoubleConsumer
  (accept [this lval]
    (set! value (.binaryDouble binop value lval))
    (set! n-elems (unchecked-inc n-elems)))
  Reducible
  (reduce [this rhs]
    (set! value (.binaryDouble binop value (.-value ^BinDoubleConsumer rhs)))
    (set! n-elems (+ n-elems (.-n-elems ^BinDoubleConsumer rhs)))
    this)
  IDeref
  (deref [this] {:value value :n-elems n-elems}))


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
                         0
                         reducer-value)
    :else
    (errors/throwf "Connot convert value to double consumer: %s" reducer-value)))


(defn staged-double-consumer-reduction
  "A staged consumer is a consumer can be used in a map-reduce pathway
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


(defn double-summation
  "Double sum of data using
  [Kahan compensated summation](https://en.wikipedia.org/wiki/Kahan_summation_algorithm)."
  (^double [options rdr]
   (hamf/sum options (or (dtype-base/as-reader rdr :float64) rdr)))
  (^double [rdr]
   (double-summation {} rdr)))


(defn unary-double-summation
  "Perform a double summation using a unary operator to transform the input stream
  into a new double stream."
  (^double [^UnaryOperator op options rdr]
   (->> (dtype-base/->reader rdr options)
        (lznc/map (hamf/double-unary-operator
                   v (.unaryDouble op v)))
        (hamf/sum options)))
  (^double [op rdr]
   (unary-double-summation op {} rdr)))


(defmacro checked-binary-merge
  [lvar rvar code]
  `(fn [~lvar ~rvar]
     (if (and ~lvar ~rvar)
       ~code
       (or ~lvar ~rvar))))


(defn commutative-binary-double
  "Perform a commutative reduction using a binary operator to perform
  the reduction.  The operator needs to be both commutative and associative."
  (^double [^BinaryOperator op options rdr]
   (->> (or (dtype-base/->reader rdr :float64) rdr)
        (hamf/apply-nan-strategy options)
        (hamf/preduce (constantly nil)
                      (hamf/double-accumulator
                       acc v
                       (if acc
                         (.binaryDouble op acc v)
                         v))
                      (checked-binary-merge l r (.binaryDouble op (double l) (double r)))
                      options)
        (Casts/doubleCast)))
  (^double [op rdr]
   (commutative-binary-double op {} rdr)))


(defn commutative-binary-long
  "Perform a commutative reduction in int64 space using a binary operator.  The
  operator needs to be both commutative and associative."
  ^long [^BinaryOperator op rdr]
  (->> (or (dtype-base/as-reader rdr :int64) rdr)
       (hamf/preduce (constantly nil)
                      (hamf/long-accumulator
                       acc v
                       (if acc
                         (.binaryLong op acc v)
                         v))
                      (checked-binary-merge l r (.binaryLong op (long l) (long r))))
       (Casts/longCast)))


(defn commutative-binary-reduce
  [^BinaryOperator op data]
  (let [op-dtype (casting/simple-operation-space (dtype-base/elemwise-datatype data)
                                                 (get (meta op) :operational-space :object))]
    (case op-dtype
      :int64 (commutative-binary-long op data)
      :float64 (commutative-binary-double op data)
      (->> (or (dtype-base/as-reader data) data)
           (hamf/preduce (constantly nil)
                         (fn [acc v] (if acc (op acc v) v))
                         (checked-binary-merge l r (op l r)))))))
