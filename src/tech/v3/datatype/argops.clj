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
            [tech.v3.datatype.emap :as emap]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.reductions :as reductions]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.const-reader :as const-reader]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf]
            [ham-fisted.function :as hamf-fn]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.lazy-noncaching :as lznc])
  (:import [it.unimi.dsi.fastutil.ints IntArrays IntComparator]
           [it.unimi.dsi.fastutil.longs LongArrays LongComparator]
           [it.unimi.dsi.fastutil.doubles DoubleComparator]
           [clojure.lang IFn$OLO]
           [tech.v3.datatype
            Comparators$IntComp
            Comparators$LongComp
            Comparators$DoubleComp
            BinaryPredicate
            Buffer
            UnaryOperator BinaryOperator
            UnaryPredicate BinaryPredicate]
           [tech.v3.datatype.unary_pred IndexList]
           [java.util Comparator Map Iterator Collections Random LinkedHashMap Map$Entry]
           [java.util.function LongPredicate DoublePredicate Predicate LongConsumer]
           [java.util PriorityQueue]
           [org.roaringbitmap RoaringBitmap]
           [ham_fisted ArrayLists ArrayHelpers MapForward IFnDef$OLO Reductions]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


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
                       "unary operator" identity))


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
  ^Comparator [src-comparator nan-strategy values]
  (let [src-dtype (dtype-base/operational-elemwise-datatype values)
        values (dtype-cmc/ensure-reader values)
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
                    rhs-value (.readDouble values rhs)
                    lhs-nan? (Double/isNaN lhs-value)
                    rhs-nan? (Double/isNaN rhs-value)]
                (if (or lhs-nan? rhs-nan?)
                  (if (identical? nan-strategy :exception)
                    (throw (Exception. "##NaN value detected"))
                    (if (and lhs-nan? rhs-nan?)
                      0
                      (if (identical? nan-strategy :first)
                        (long (if lhs-nan? -1 1))
                        (long (if lhs-nan? 1 -1)))))
                  (.compare comp lhs-value rhs-value))))))
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


(defn find-base-comparator
  [comparator val-dtype]
  (or (if (keyword? comparator)
        (binary-pred/builtin-ops comparator)
        comparator)
      (if (casting/numeric-type? val-dtype)
        (binary-pred/builtin-ops :tech.numerics/<)
        compare)))


(defn argsort
  "Sort values in index space returning a buffer of indexes.  By default uses a parallelized
  quicksort algorithm by default.


  * `compare-fn` may be one of:
     - a clojure operator like clojure.core/<
     - `:tech.numerics/<`, `:tech.numerics/>` for unboxing comparisons of primitive
        values.
     - clojure.core/compare
     - A custom java.util.Comparator instantiation.

  Options:

  * `:nan-strategy` - General missing strategy.  Options are `:first`, `:last`, and
    `:exception`.
  * `:parallel?` - Uses parallel quicksort when true and regular quicksort when false."
  ([comparator {:keys [parallel?
                       nan-strategy]
                :or {parallel? true
                     nan-strategy :last}
                :as _options}
    values]
   (let [n-elems (dtype-base/ecount values)
         val-dtype (dtype-base/operational-elemwise-datatype values)
         comparator (-> (find-base-comparator comparator val-dtype)
                        (index-comparator nan-strategy values))]
     (-> (cond
           (== n-elems 0)
           (ArrayLists/toList (int-array 0))
           (instance? IntComparator comparator)
           (let [^ints idx-ary (hamf/iarange n-elems)]
             (if parallel?
               (IntArrays/parallelQuickSort idx-ary ^IntComparator comparator)
               (IntArrays/quickSort idx-ary ^IntComparator comparator))
             (ArrayLists/toList idx-ary))
           :else
           (let [^longs idx-ary (hamf/larange (range n-elems))]
             (if parallel?
               (LongArrays/parallelQuickSort idx-ary ^LongComparator comparator)
               (LongArrays/quickSort idx-ary ^LongComparator comparator))
             (ArrayLists/toList idx-ary)))
         (with-meta {:min 0 :max n-elems}))))
  ([comparator values]
   (argsort comparator {} values))
  ([values]
   (argsort nil {} values)))


(defn arg-min-n
  "Return the indexes of the top minimum items.  Values must be countable and random access.
  Same options,arguments as [[argsort]]."
  ([N comparator {:keys [nan-strategy]
                  :or {nan-strategy :last}}
    values]
   (let [N (long N)
         val-dtype (dtype-base/operational-elemwise-datatype values)
         comparator (-> (find-base-comparator comparator val-dtype)
                        (index-comparator nan-strategy values))
         queue (PriorityQueue. (int N) (.reversed comparator))
         n-elems (dtype-base/ecount values)]
     (if (instance? IntComparator comparator)
       (dotimes [idx n-elems]
         (.offer queue (unchecked-int idx))
         (when (> (.size queue) N)
           (.poll queue)))
       (dotimes [idx n-elems]
         (.add queue idx)
         (when (> (.size queue) N)
           (.poll queue))))
     (reduce (hamf-rf/indexed-accum
              acc idx v
              (ArrayHelpers/aset ^ints acc idx (unchecked-int v))
              acc)
             (int-array (.size queue))
             queue)))
  ([N comparator values] (arg-min-n N comparator nil values))
  ([N values] (arg-min-n N nil nil values)))


(defmacro ^:private double-compare
  [_comp lhs rhs]
  `(Double/compare ~lhs ~rhs))


(defmacro ^:private long-compare
  [_comp lhs rhs]
  `(Long/compare ~lhs ~rhs))


(defmacro ^:private object-compare
  [comp lhs rhs]
  `(.compare ~comp ~lhs ~rhs))


(defmacro binary-search-impl
  [data target scalar-cast read-fn comparator compare-fn]
  `(let [data# (dtype-base/->buffer ~data)
         target# (~scalar-cast ~target)
         n-elems# (.lsize data#)]
     (loop [low# (long 0)
            high# n-elems#]
       (if (< low# high#)
         (let [mid# (+ low# (quot (- high# low#) 2))
               buf-data# (~read-fn data# mid#)
               compare-result# (~compare-fn ~comparator buf-data# target#)]
           (if (= 0 compare-result#)
             (recur mid# mid#)
             (if (and (< compare-result# 0)
                      (not= mid# low#))
               (recur mid# high#)
               (recur low# mid#))))
         (loop [low# low#]
           (let [buf-data# (~read-fn data# low#)
                 comp# (~compare-fn ~comparator target# buf-data#)]
             (cond
               (or (< comp# 0) (== 0 low#)) low#
               (> comp# 0) (unchecked-inc low#)
               ;;When values are equal, track backward to first non-equal member.
               :else
               (recur (unchecked-dec low#)))))))))


(defn binary-search
  "Returns a long result that points to just before the value or exactly points to the
   value.  In the case where the target is after the last value will return
  elem-count.

  Options:

  * `:comparator` - a specific comparator to use; defaults to `comparator`."
  (^long [data target options]
   (let [opt-dtype (:datatype options (dtype-base/elemwise-datatype data))
         dtype (casting/simple-operation-space opt-dtype)
         ;;If any special comparator is passed in then default to object
         ;;space binary search.  Else we have fastpaths for natural ordering
         ;;for doubles and longs
         dtype (if (identical? :tech.numerics/<
                               (:comparator options :tech.numerics/<))
                 dtype
                 :object)]
     (case dtype
       :float64 (binary-search-impl data target double .readDouble nil
                                    double-compare)
       :int64 (binary-search-impl data target long .readLong nil
                                  long-compare)
       :object (let [comparator (-> (find-base-comparator (:comparator options)
                                                          opt-dtype)
                                    (->comparator))]
                 (binary-search-impl data target identity .readObject
                                     comparator object-compare)))))
  (^long [data target]
   (binary-search data target nil)))


(defn argfilter
  "Filter out values returning either an iterable of indexes or a reader
  of indexes."
  ([pred options rdr]
   (if-let [rdr (dtype-base/as-reader rdr)]
     (let [n-elems (.lsize rdr)]
       (cond
         (instance? LongPredicate pred)
         (hamf-rf/preduce-reducer
          (unary-pred/index-reducer (get options :storage-type n-elems)) options
          (->> (hamf/range n-elems)
               (lznc/filter (hamf-fn/long-predicate
                             v (.test ^LongPredicate pred (.readLong rdr v))))))
         (instance? DoublePredicate pred)
         (hamf-rf/preduce-reducer
          (unary-pred/index-reducer (get options :storage-type n-elems)) options
          (->> (hamf/range n-elems)
               (lznc/filter (hamf-fn/long-predicate
                             v (.test ^DoublePredicate pred (.readDouble rdr v))))))
         (instance? Predicate pred)
         (hamf-rf/preduce-reducer
          (unary-pred/index-reducer (get options :storage-type n-elems)) options
          (->> (hamf/range n-elems)
               (lznc/filter (hamf-fn/long-predicate
                             v (.test ^Predicate pred (.readObject rdr v))))))
         :else
         (let [pred (->unary-predicate pred)]
           (->> (unary-pred/reader pred rdr)
                (unary-pred/bool-reader->indexes options)))))
     (let [pred (unary-pred/->predicate pred)]
       (->> rdr
            (lznc/map-indexed (fn [idx data] (when (.unaryObject pred data) idx)))
            (lznc/remove nil?)))))
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


(defn arggroup
  "Group by elemens in the reader returning a map of value->list of indexes.


  Note the options are passed through to hamf/group-by-reducer which then passes
  them through to preduce-reducer.

  Options:

  - `:storage-datatype` - `:int32`, `:int64, or `:bitmap`, defaults to whatever will fit
    based on the element count of the reader.
  - `:unordered?` - defaults to true, if true uses a slower algorithm that guarantees
    the resulting index lists will be ordered.  In the case where storage is
    bitmap, unordered reductions are used as the bitmap forces the end results to be
    ordered
  - `:key-fn` - defaults to identity.  In this case the reader's values are used as the
    keys."
  (^Map [{:keys [storage-datatype unordered? skip-finalize? map-fn operation-space] :as options} rdr]
   (when-not (dtype-base/reader? rdr)
     (errors/throwf "Input must be convertible to a reader"))
   ;;This function can be written fairly trivially with hamf/group-by-consumer but
   ;;since we know we are working in index space we can write it this way and get
   ;;some improvement because we do not have to index into the reader with
   ;;getObject calls, we can call the readers reduce method and thus get fast reduction
   ;;over the input object
   (let [storage-datatype (or storage-datatype (unary-pred/reader-index-space rdr))
         n-elems (dtype-base/ecount rdr)
         idx-rdr (unary-pred/index-reducer storage-datatype)
         init-fn (hamf-proto/->init-val-fn idx-rdr)
         ^IFn$OLO rfn (hamf-proto/->rfn idx-rdr)
         afn (hamf-fn/function k (init-fn))
         merge-bifn (hamf-fn/->bi-function (hamf-proto/->merge-fn idx-rdr))
         map-merge #(hamf/mut-map-union! merge-bifn %1 %2)
         op-space (or operation-space (casting/simple-operation-space (dtype-base/elemwise-datatype rdr)))
         fin-fn (if skip-finalize?
                  identity
                  (fn [^Map m]
                    (do (hamf-rf/preduce
                         (constantly nil)
                         (fn [acc ^Map$Entry e]
                           (.setValue e (hamf-proto/finalize idx-rdr (.getValue e))))
                         (fn [l r] l)
                         m)
                        m)))
         map-fn (or map-fn hamf/linked-hashmap)]
     (->> (hamf/pgroups
           n-elems
           (fn [^long sidx ^long eidx]
             (reduce (case op-space
                       :int64 (hamf-rf/indexed-long-accum
                               acc idx v
                               (let [l (.computeIfAbsent ^Map acc v afn)]
                                 (.invokePrim rfn l (+ sidx idx))
                                 acc))
                       :float64 (hamf-rf/indexed-double-accum
                                 acc idx v
                                 (let [l (.computeIfAbsent ^Map acc v afn)]
                                   (.invokePrim rfn l (+ sidx idx))
                                   acc))
                       (hamf-rf/indexed-accum
                        acc idx v
                        (let [l (.computeIfAbsent ^Map acc v afn)]
                          (.invokePrim rfn l (+ sidx idx))
                          acc)))
                     (map-fn)
                     (dtype-base/sub-buffer rdr sidx (- eidx sidx))))
           {:min-n 1000})
          (Reductions/iterableMerge (hamf-rf/options->parallel-options {:min-n 1000}) map-merge)
          (fin-fn))))
  (^Map [rdr]
   (arggroup nil rdr)))


(defn arggroup-by
  "Group by elemens in the reader returning a map of value->list of indexes. Indexes
  may not be ordered.  :storage-datatype may be specific in the options to set
  the datatype of the indexes else the system will decide based on reader length.
  See arggroup for Options."
  (^Map [partition-fn options rdr]
   (arggroup options (if partition-fn
                       (emap/emap partition-fn nil rdr)
                       rdr)))
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
