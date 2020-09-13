(ns tech.v3.datatype.argops
  (:require [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.copy-make-container :as dtype-cmc])
  (:import [it.unimi.dsi.fastutil.bytes ByteArrays ByteComparator]
           [it.unimi.dsi.fastutil.shorts ShortArrays ShortComparator]
           [it.unimi.dsi.fastutil.ints IntArrays IntComparator]
           [it.unimi.dsi.fastutil.longs LongArrays LongComparator]
           [it.unimi.dsi.fastutil.floats FloatArrays FloatComparator]
           [it.unimi.dsi.fastutil.doubles DoubleArrays DoubleComparator]
           [tech.v3.datatype
            Comparators$IntComp
            Comparators$LongComp
            Comparators$FloatComp
            Comparators$DoubleComp
            BinaryPredicate
            PrimitiveList]
           [java.util Comparator Arrays List]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ->long-comparator
  ^LongComparator [src-comparator]
  (cond
    (instance? Comparators$LongComp src-comparator)
    src-comparator
    (instance? BinaryPredicate src-comparator)
    (.asLongComparator ^BinaryPredicate src-comparator)
    :else
    (let [^Comparator comp (comparator src-comparator)]
      (reify Comparators$LongComp
        (compareLongs [this lhs rhs]
          (.compare comp lhs rhs))))))


(defn ->double-comparator
  ^DoubleComparator [src-comparator]
  (cond
    (instance? Comparators$DoubleComp src-comparator)
    src-comparator
    (instance? BinaryPredicate src-comparator)
    (.asDoubleComparator ^BinaryPredicate src-comparator)
    :else
    (let [^Comparator comp (comparator src-comparator)]
      (reify Comparators$DoubleComp
        (compareDoubles [this lhs rhs]
          (.compare comp lhs rhs))))))


(defn ->comparator
  ^Comparator [src-comparator]
  (cond
    (instance? Comparator src-comparator)
    src-comparator
    (instance? BinaryPredicate src-comparator)
    (.asComparator ^BinaryPredicate src-comparator)
    :else
    (comparator src-comparator)))


(defn index-comparator
  [values src-comparator]
  (let [src-dtype (dtype-base/elemwise-datatype values)
        values (dtype-base/ensure-reader values)
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
              (let [lhs-value (.readDouble values lhs)
                    rhs-value (.readDouble values rhs)]
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
              (let [lhs-value (.readDouble values lhs)
                    rhs-value (.readDouble values rhs)]
                (.compare comp lhs-value rhs-value)))))))))


(defn argsort
  ([comparator {:keys [parallel?]
                 :or {parallel? true}}
    values]
   (let [n-elems (dtype-base/ecount values)
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
   (argsort comparator {}  values))
  ([values]
   (argsort (binary-pred/builtin-ops :<) {}  values)))



(defn argfilter
  [pred options rdr]
  (if-let [rdr (dtype-base/as-reader rdr)]
    (unary-pred/bool-reader->indexes options (unary-pred/reader pred rdr))
    (let [pred (unary-pred/->unary-predicate pred)]
      (->> rdr
           (map-indexed (fn [idx data]
                          (when (.unaryObject pred data) idx)))
           (remove nil?)))))


(defn binary-argfilter
  [pred options lhs rhs]
  (let [lhs (dtype-base/as-reader lhs)
        rhs (dtype-base/as-reader rhs)]
    (if (and lhs rhs)
      (unary-pred/bool-reader->indexes options (binary-pred/reader pred lhs rhs))
      (let [pred (binary-pred/->predicate pred)]
        (map (fn [idx lhs rhs]
               (when (.binaryObject pred lhs rhs) idx))
             (range) lhs rhs)
        (remove nil?)))))
