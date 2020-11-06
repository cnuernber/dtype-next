(ns tech.v3.datatype.argops
  "Efficient functions for operating in index space.  Take-off of the argsort, argmin, etc.
  type functions from Matlab.  These functions generally only work on readers and all return
  some version of an index or list of indexes."
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.reductions :as reductions]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.const-reader :as const-reader]
            [primitive-math :as pmath])
  (:import [it.unimi.dsi.fastutil.bytes ByteComparator]
           [it.unimi.dsi.fastutil.shorts ShortComparator]
           [it.unimi.dsi.fastutil.ints IntArrays IntComparator]
           [it.unimi.dsi.fastutil.longs LongArrays LongComparator]
           [it.unimi.dsi.fastutil.floats FloatComparator]
           [it.unimi.dsi.fastutil.doubles DoubleComparator]
           [tech.v3.datatype
            Comparators$IntComp
            Comparators$LongComp
            Comparators$FloatComp
            Comparators$DoubleComp
            BinaryPredicate
            PrimitiveList
            IndexReduction
            Buffer
            UnaryOperator BinaryOperator
            UnaryPredicate BinaryPredicate]
           [java.util Comparator Arrays List Map Iterator Collections Random]
           [org.roaringbitmap RoaringBitmap]))


(set! *warn-on-reflection* true)


(defn ensure-reader
  "Ensure item is randomly addressable.  This may copy the data into a randomly
  accessible container."
  (^Buffer [item n-const-elems]
   (let [argtype (argtypes/arg-type item)]
     (cond
       (= argtype :scalar)
       (const-reader/const-reader item n-const-elems)
       (= argtype :iterable)
       (-> (dtype-cmc/make-container :list (dtype-base/elemwise-datatype item) {}
                                     item)
           (dtype-base/->reader))
       :else
       (dtype-base/->reader item))))
  (^Buffer [item]
   (ensure-reader item Long/MAX_VALUE)))


(defn tech-numerics-kwd?
  [kwd]
  (and (keyword? kwd)
       (= (namespace kwd) "tech.numerics")))


(defn- find-unary-operator
  [op op-map optypename opconversion-fn]
  (or
   (when (tech-numerics-kwd? op)
     (if-let [retval (get op-map op)]
       retval
       (errors/throwf "Failed to find %s %s" optypename op)))
   (opconversion-fn op)))


(defn- find-binary-operator
  [op op-map optypename opconversion-fn]
  (or
   (when (tech-numerics-kwd? op)
     (if-let [retval (get op-map op)]
       retval
       (errors/throwf "Failed to find %s %s" optypename op)))
   (opconversion-fn op)))



(defn ->unary-operator
  "Convert a thing to a unary operator. Thing can be a keyword or
  an implementation of IFn or an implementation of a UnaryOperator."
  ^UnaryOperator [op]
  (find-unary-operator op unary-op/builtin-ops
                       "unary operator" unary-op/->operator))


(defn ->binary-operator
  "Convert a thing to a binary operator.  Thing can be a keyword or
  an implementation of IFn or an implementation of a BinaryOperator."
  ^BinaryOperator [op]
  (find-binary-operator op binary-op/builtin-ops
                        "binary operator" binary-op/->operator))


(defn ->unary-predicate
  "Convert a thing to a unary predicate. Thing can be a keyword or
  an implementation of IFn or an implementation of a UnaryPredicate."
  ^UnaryPredicate [op]
  (find-unary-operator op unary-pred/builtin-ops
                       "unary predicate" unary-pred/->predicate))


(defn ->binary-predicate
  "Convert a thing to a binary predicate.  Thing can be a keyword or
  an implementation of IFn or an implementation of a BinaryPredicate."
  ^BinaryPredicate [op]
  (find-binary-operator op binary-pred/builtin-ops
                        "binary predicate" binary-pred/->predicate))


(def ^:private compare-compile-time-family
  {:int64 {:min-value 'Long/MIN_VALUE :max-value 'Long/MAX_VALUE
           :compare 'Long/compare :read-fn '.readLong :pred-fn '.binaryLong}
   :float64 {:min-value 'Double/MIN_VALUE :max-value 'Double/MAX_VALUE
             :compare 'Double/compare :read-fn '.readDouble :pred-fn '.binaryDouble}
   :object {:min-value 'nil :max-value 'nil
            :compare 'compare :read-fn '.readObject :pred-fn '.binaryObject}})


(defmacro ^:private impl-arglast-every-loop
  [datatype n-elems rdr pred]
  (let [{:keys [read-fn pred-fn]} (compare-compile-time-family datatype)]
    (when-not (and read-fn pred-fn)
      (throw (Exception. (format "Compile failure: %s, :read-fn %s, :pred-fn %s"
                                 datatype read-fn pred-fn))))
    `(loop [idx# 1
            max-idx# 0
            max-value# (~read-fn ~rdr 0)]
       (if (== ~n-elems idx#)
         max-idx#
         (let [cur-val# (~read-fn ~rdr idx#)
               found?# (~pred-fn ~pred cur-val# max-value#)]
           (recur (unchecked-inc idx#)
                  (if found?# idx# max-idx#)
                  (if found?# cur-val# max-value#)))))))



(defn arglast-every
  "Return the last index where (pred (rdr idx) (rdr (dec idx))) was true by
  comparing every value and keeping track of the last index where pred was true."
  [rdr pred-op]
  (let [pred (->binary-predicate pred-op)
        op-space (casting/simple-operation-space
                  (dtype-base/elemwise-datatype rdr))
        rdr (dtype-base/->reader rdr op-space)
        n-elems (.lsize rdr)]
    (case op-space
      :int64 (impl-arglast-every-loop :int64 n-elems rdr pred)
      :float64 (impl-arglast-every-loop :float64 n-elems rdr pred)
      (impl-arglast-every-loop :object n-elems rdr pred))))


(defn argmax
  "Return the index of the max item in the reader."
  ^long [rdr]
  (arglast-every rdr :tech.numerics/>))


(defn argmin
  "Return the index of the min item in the reader."
  ^long [rdr]
    (arglast-every rdr :tech.numerics/<))

(defmacro impl-index-of
  [datatype comp-value n-elems pred rdr]
  (let [{:keys [read-fn pred-fn]} (compare-compile-time-family datatype)]
    `(let [comp-value# (casting/datatype->cast-fn :unknown ~datatype ~comp-value)]
       (loop [idx# 0]
         (if (or (== idx# ~n-elems)
                 (~pred-fn ~pred (~read-fn ~rdr idx#) comp-value#))
           idx#
           (recur (unchecked-inc idx#)))))))


(defn index-of
  "Return the first time pred is true given a comparison value."
  (^long [rdr pred value]
   (let [pred (->binary-predicate pred)
         op-space (casting/simple-operation-space
                   (dtype-base/elemwise-datatype rdr))
         rdr (dtype-base/->reader rdr op-space)
         n-elems (.lsize rdr)]
     (case op-space
       :int64 (impl-index-of :int64 value n-elems pred rdr)
       :float64 (impl-index-of :float64 value n-elems pred rdr)
       (impl-index-of :object value n-elems pred rdr))))
  (^long [rdr value]
   (index-of rdr :tech.numerics/eq value)))

(defmacro ^:private impl-last-idx-of
  [datatype comp-value n-elems pred rdr]
  (let [{:keys [read-fn pred-fn]} (compare-compile-time-family datatype)]
    `(let [comp-value# (casting/datatype->cast-fn :unknown ~datatype ~comp-value)
           n-elems-dec# (dec ~n-elems)]
       (loop [idx# 0]
         (if (or (== idx# ~n-elems)
                 (~pred-fn ~pred (~read-fn ~rdr (- n-elems-dec# idx#)) comp-value#))
           (- n-elems-dec# idx#)
           (recur (unchecked-inc idx#)))))))


(defn last-index-of
  "Return the last index of the last time time pred, which defaults to :tech.numerics/eq, is
  true."
  (^long [rdr pred value]
   (let [op-space (casting/simple-operation-space
                   (dtype-base/elemwise-datatype rdr))
         rdr (dtype-base/->reader rdr op-space)
         n-elems (.lsize rdr)
         pred (->binary-predicate pred)]
     (case op-space
       :int64 (impl-last-idx-of :int64 value n-elems pred rdr)
       :float64 (impl-last-idx-of :float64 value n-elems pred rdr)
       (impl-last-idx-of :object value n-elems pred rdr))))
  (^long [rdr value]
   (last-index-of rdr :tech.numerics/eq value)))

(defn ->comparator
  "Convert a thing to a java.util.Comparator."
  ^Comparator [src-comparator]
  (cond
    (instance? Comparator src-comparator)
    src-comparator
    (instance? BinaryPredicate src-comparator)
    (.asComparator ^BinaryPredicate src-comparator)
    :else
    (comparator src-comparator)))


(defn ->long-comparator
  "Convert a thing to a it.unimi.dsi.fastutil.longs.LongComparator."
  ^LongComparator [src-comparator]
  (cond
    (instance? LongComparator src-comparator)
    src-comparator
    (instance? BinaryPredicate src-comparator)
    (.asLongComparator ^BinaryPredicate src-comparator)
    :else
    (let [^Comparator src-comparator (->comparator src-comparator)]
      (reify Comparators$LongComp
        (compareLongs [this lhs rhs]
          (.compare src-comparator lhs rhs))))))


(defn ->double-comparator
  "Convert a thing to a it.unimi.dsi.fastutil.doubles.DoubleComparator."
  ^DoubleComparator [src-comparator]
  (cond
    (instance? DoubleComparator src-comparator)
    src-comparator
    (instance? BinaryPredicate src-comparator)
    (.asDoubleComparator ^BinaryPredicate src-comparator)
    :else
    (let [^Comparator src-comparator (->comparator src-comparator)]
      (reify Comparators$DoubleComp
        (compareDoubles [this lhs rhs]
          (.compare src-comparator lhs rhs))))))


(defn index-comparator
  "Given a reader of values an a source comparator, return either an
  IntComparator or a LongComparator depending on the number of indexes
  in the reader that compares the values using the passed in comparator."
  [values src-comparator]
  (let [src-dtype (dtype-base/elemwise-datatype values)
        values (ensure-reader values)
        n-values (.lsize values)
        src-comparator (if-let [bin-pred (:binary-predicate (meta src-comparator))]
                         (binary-pred/builtin-ops bin-pred)
                         src-comparator)]
    (if (< n-values Integer/MAX_VALUE)
      (cond
        (casting/integer-type? src-dtype)
        (let [comp (->long-comparator src-comparator)]
          (reify Comparators$IntComp
            (compareInts [this lhs rhs]
              (let [lhs-value (.readLong values lhs)
                    rhs-value (.readLong values rhs)]
                (.compare comp lhs-value rhs-value)))))
        (casting/float-type? src-dtype)
        (let [comp (->double-comparator src-comparator)]
          (reify Comparators$IntComp
            (compareInts [this lhs rhs]
              (let [lhs-value (.readDouble values lhs)
                    rhs-value (.readDouble values rhs)]
                (.compare comp lhs-value rhs-value)))))
        :else
        (let [^Comparator comp (->comparator src-comparator)]
          (reify Comparators$IntComp
            (compareInts [this lhs rhs]
              (let [lhs-value (.readObject values lhs)
                    rhs-value (.readObject values rhs)]
                (.compare comp lhs-value rhs-value))))))
      (cond
        (casting/integer-type? src-dtype)
        (let [comp (->long-comparator src-comparator)]
          (reify Comparators$LongComp
            (compareLongs [this lhs rhs]
              (let [lhs-value (.readLong values lhs)
                    rhs-value (.readLong values rhs)]
                (.compare comp lhs-value rhs-value)))))
        (casting/float-type? src-dtype)
        (let [comp (->double-comparator src-comparator)]
          (reify Comparators$LongComp
            (compareLongs [this lhs rhs]
              (let [lhs-value (.readDouble values lhs)
                    rhs-value (.readDouble values rhs)]
                (.compare comp lhs-value rhs-value)))))
        :else
        (let [^Comparator comp (->comparator src-comparator)]
          (reify Comparators$LongComp
            (compareLongs [this lhs rhs]
              (let [lhs-value (.readObject values lhs)
                    rhs-value (.readObject values rhs)]
                (.compare comp lhs-value rhs-value)))))))))


(defn argsort
  "Sort values in index space.  By default uses a parallelized quicksort algorithm."
  ([comparator {:keys [parallel?]
           :or {parallel? true}}
    values]
   (let [n-elems (dtype-base/ecount values)
         val-dtype (dtype-base/elemwise-datatype values)
         comparator (or (if (keyword? comparator)
                          (binary-pred/builtin-ops comparator)
                          comparator)
                        (if (casting/numeric-type? val-dtype)
                          (binary-pred/builtin-ops :tech.numerics/<)
                          compare))
         comparator (index-comparator values comparator)]
     (cond
       (== n-elems 0)
       (int-array 0)
       (instance? IntComparator comparator)
       (let [^ints idx-ary (dtype-cmc/->array :int32 (range n-elems))]
         (if parallel?
           (IntArrays/parallelQuickSort idx-ary ^IntComparator comparator)
           (IntArrays/quickSort idx-ary ^IntComparator comparator))
         idx-ary)
       :else
       (let [^longs idx-ary (dtype-cmc/->array :int64 (range n-elems))]
         (if parallel?
           (LongArrays/parallelQuickSort idx-ary ^LongComparator comparator)
           (LongArrays/quickSort idx-ary ^LongComparator comparator))
         idx-ary))))
  ([comparator values]
   (argsort comparator {} values))
  ([values]
   (argsort nil {} values)))


(defn argfilter
  "Filter out values returning either an iterable of indexes or a reader
  of indexes."
  ([pred options rdr]
   (if-let [rdr (dtype-base/as-reader rdr)]
     (unary-pred/bool-reader->indexes options
                                      (unary-pred/reader (->unary-predicate pred) rdr))
     (let [pred (unary-pred/->predicate pred)]
       (->> rdr
            (map-indexed (fn [idx data]
                           (when (.unaryObject pred data) idx)))
            (remove nil?)))))
  ([pred rdr]
   (argfilter pred nil rdr)))


(defn binary-argfilter
  "Filter out values using a binary predicate.  Returns either an iterable of indexes
  or a reader of indexes."
  ([pred options lhs rhs]
   (if (and (dtype-base/reader? lhs)
            (dtype-base/reader? rhs))
     (unary-pred/bool-reader->indexes options (binary-pred/reader
                                               (->binary-predicate pred)
                                               lhs rhs))
     (let [pred (->binary-predicate pred)]
       (map (fn [idx lhs rhs]
              (when (.binaryObject pred lhs rhs) idx))
            (range)
            (dtype-base/->iterable lhs)
            (dtype-base/->iterable rhs))
       (remove nil?))))
  ([pred lhs rhs]
   (binary-argfilter pred nil lhs rhs)))


(defn index-reducer
  "Create an implementation of an tech.v3.datatype.IndexReduction interface that stores
  the indexes in a particular storage type.  Storage types may be :int32 :int64 or, for
  storing the result in a RoaringBitmap, :bitmap."
  ^IndexReduction [storage-datatype]
  (if-not (= :bitmap storage-datatype)
    (reify IndexReduction
      (reduceIndex [this batch-data ctx idx]
        (let [^PrimitiveList ctx (if ctx
                                   ctx
                                   (dtype-list/make-list storage-datatype))]
          (.addLong ctx idx)
          ctx))
      (reduceReductions [this lhs-ctx rhs-ctx]
        (.addAll ^List lhs-ctx rhs-ctx)
        lhs-ctx))
    (reify IndexReduction
      (reduceIndex [this batch-data ctx idx]
        (let [^RoaringBitmap  ctx (if ctx
                                    ctx
                                    (RoaringBitmap.))]
          (.add ctx (unchecked-int idx))
          ctx))
      (reduceReductions [this lhs-ctx rhs-ctx]
        (.or ^RoaringBitmap lhs-ctx ^RoaringBitmap rhs-ctx)
        lhs-ctx))))


(defn arggroup
  "Group by elemens in the reader returning a map of value->list of indexes.

  Options:

  - storage-datatype - :int32, :int64, or :bitmap, defaults to whatever will fit
    based on the element count of the reader.
  - unordered? - defaults to true, if true uses a slower algorithm that guarantees
    the resulting index lists will be ordered.  In the case where storage is
    bitmap, unordered reductions are used as the bitmap forces the end results to be
    ordered"
  (^Map [{:keys [storage-datatype unordered?]} rdr]
   (when-not (dtype-base/reader? rdr)
     (errors/throwf "Input must be convertible to a reader"))
   (let [storage-datatype (or storage-datatype (unary-pred/reader-index-space rdr))
         unordered? (if (= storage-datatype :bitmap)
                      true
                      unordered?)
         reducer (index-reducer storage-datatype)]
     (if-not unordered?
       (reductions/ordered-group-by-reduce reducer rdr)
       (reductions/unordered-group-by-reduce reducer rdr))))
  (^Map [rdr]
   (arggroup nil rdr)))


(defn arggroup-by
  "Group by elemens in the reader returning a map of value->list of indexes. Indexes
  may not be ordered.  :storage-datatype may be specific in the options to set
  the datatype of the indexes else the system will decide based on reader length.
  See arggroup for Options."
  (^Map [partition-fn options rdr]
   (if (= identity partition-fn)
     (arggroup options rdr)
     (arggroup options (unary-op/reader (->unary-operator partition-fn) rdr))))
  (^Map [partition-fn rdr]
   (arggroup-by partition-fn nil rdr)))


(defn- do-argpartition-by
  [^long start-idx ^Iterator item-iterable first-item ^BinaryPredicate pred]
  (let [[end-idx next-item]
        (loop [cur-idx start-idx]
          (let [has-next? (.hasNext item-iterable)
                next-item (when has-next? (.next item-iterable))]
            (if (and has-next? (pred first-item next-item))
              (recur (inc cur-idx))
              [cur-idx next-item])))
        end-idx (inc (long end-idx))]
    (cons [first-item (range (long start-idx) end-idx)]
          (when (.hasNext item-iterable)
            (lazy-seq (do-argpartition-by end-idx item-iterable next-item pred))))))


(defn argpartition
  "Returns a sequence of [partition-key index-reader].  Index generation is not
  parallelized.  This design allows group-by and partition-by to be used
  interchangeably (if pred is :tech.numerics/eq) as they both result in a sequence of
  [partition-key idx-reader]. This design is lazy."
  (^Iterable [pred item-iterable]
   (let [iterator (.iterator ^Iterable (dtype-base/->iterable item-iterable))]
     (when (.hasNext iterator)
       (do-argpartition-by 0 iterator (.next iterator) (->binary-predicate pred)))))
  (^Iterable [item-iterable]
   (argpartition :tech.numerics/eq item-iterable)))


(defn argpartition-by
  "Returns a sequence of [partition-key index-reader].  Index generation is not
  parallelized.  This design allows group-by and partition-by to be used
  interchangeably as they both result in a sequence of [partition-key idx-reader].
  This design is lazy."
  (^Iterable [unary-op partition-pred item-iterable]
   (let [iterator (->> (dtype-base/->iterable item-iterable)
                       (unary-op/iterable (->unary-operator unary-op))
                       (.iterator))]
     (when (.hasNext iterator)
       (do-argpartition-by 0 iterator (.next iterator)
                           (->binary-predicate partition-pred)))))
  (^Iterable [unary-op item-iterable]
   (argpartition-by unary-op :tech.numerics/eq item-iterable)))


(defn argshuffle
  "Serially shuffle N indexes into a an array of data.
  Returns an array of indexes.

  Options:

  * `:seed` - Either nil, an integer, or an implementation of `java.util.Random`.
    This seeds the random generator if provided or a new one is created if not.
  * `:container-type` - The container type of the data, defaults to `:jvm-heap`.
    See documentation for `make-container`."
  ([^long n-indexes {:keys [seed container-type]
                     :or {container-type :jvm-heap}}]
   (let [data (if (< n-indexes (long Integer/MAX_VALUE))
                (dtype-cmc/make-container container-type :int32 (range n-indexes))
                (dtype-cmc/make-container container-type :int64 (range n-indexes)))
         ^Random rgen (when seed
                        (if (number? seed)
                          (java.util.Random. (int seed))
                          seed))
         data-buf (dtype-base/->buffer data)]
     (if rgen
       (Collections/shuffle data-buf rgen)
       (Collections/shuffle data-buf))
     data))
  ([n-indexes]
   (argshuffle n-indexes nil)))
