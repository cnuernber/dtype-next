(ns tech.v3.datatype.emap
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred])
  (:import [java.util List]
           [tech.v3.datatype BooleanReader LongReader DoubleReader ObjectReader
            NumericConversions BooleanConversions]))


(defmacro ^:private cast-or-missing
  [expr cast-fn missing-val]
  `(if-let [retval# ~expr]
     (~cast-fn retval#)
     ~missing-val))


(defn- emap-reader
  [map-fn res-dtype cast-fn shapes args]
  (let [n-elems (long (dtype-base/ecount (first args)))
        ^List args (mapv #(argops/ensure-reader % n-elems) args)
        argcount (.size args)]
    (if (= res-dtype :boolean)
      (case argcount
        1 (let [arg (first args)]
            (reify BooleanReader
              (lsize [rdr] n-elems)
              (readBoolean [rdr idx]
                (cast-or-missing (map-fn (arg idx)) BooleanConversions/from false))))
        2 (let [arg1 (first args)
                arg2 (second args)]
            (reify BooleanReader
              (lsize [rdr] n-elems)
              (readBoolean [rdr idx]
                (cast-or-missing (map-fn (arg1 idx) (arg2 idx))
                                 BooleanConversions/from false))))
        (reify BooleanReader
          (lsize [rdr] n-elems)
          (readBoolean [rdr idx]
            (cast-or-missing (apply map-fn (map #(% idx) args))
                             BooleanConversions/from false))))
      ;;The unary-op/binary-op reader pathways force the operation to happen in the same
      ;;space as the return value.  For a generalized element map, all we can know is
      ;;the final value results in that space but everything else should happen in
      ;;object space.
      (cond
        (casting/integer-type? res-dtype)
        (case argcount
          1 (let [arg (first args)]
              (reify LongReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readLong [rdr idx]
                  (cast-or-missing (map-fn (arg idx)) NumericConversions/longCast
                                   Long/MIN_VALUE))))
          2 (let [lhs (first args)
                  rhs (second args)]
              (reify LongReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readLong [rdr idx]
                  (cast-or-missing (map-fn (lhs idx) (rhs idx))
                                   NumericConversions/longCast
                                   Long/MIN_VALUE))))
          ;;default
          (reify LongReader
            (elemwiseDatatype [rdr] res-dtype)
            (lsize [rdr] n-elems)
            (readLong [rdr idx]
              (cast-or-missing (apply map-fn (map #(% idx) args))
                               NumericConversions/longCast
                               Long/MIN_VALUE))))
        (casting/float-type? res-dtype)
        (case argcount
          1 (let [arg (first args)]
              (reify DoubleReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readDouble [rdr idx]
                  (cast-or-missing (map-fn (arg idx))
                                   NumericConversions/doubleCast
                                   Double/NaN))))
          2 (let [lhs (first args)
                  rhs (second args)]
              (reify DoubleReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readDouble [rdr idx]
                  (cast-or-missing (map-fn (lhs idx) (rhs idx))
                                   NumericConversions/doubleCast
                                   Double/NaN))))
          (reify DoubleReader
            (elemwiseDatatype [rdr] res-dtype)
            (lsize [rdr] n-elems)
            (readDouble [rdr idx]
              (cast-or-missing (apply map-fn (map #(% idx) args))
                               NumericConversions/doubleCast
                               Double/NaN))))
        :else
        (case argcount
          1 (let [arg (first args)]
              (reify ObjectReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readObject [rdr idx]
                  (cast-fn (map-fn (arg idx))))))
          2 (let [lhs (first args)
                  rhs (second args)]
              (reify ObjectReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readObject [rdr idx]
                  (cast-fn (map-fn (lhs idx) (rhs idx))))))
          (reify ObjectReader
            (elemwiseDatatype [rdr] res-dtype)
            (lsize [rdr] n-elems)
            (readObject [rdr idx]
              (cast-fn
               (apply map-fn (map #(% idx) args))))))))))


(defn emap
  "Elemwise map:

  1. If input are all scalars, results in a scalar.
  2. If any inputs are iterables, results in an iterable.
  3. Either a reader or a tensor is returned.  All input shapes
     have to match.

  res-dtype is nil it is deduced from unifying the argument datatypes"
  [map-fn res-dtype & args]
  (let [res-dtype (or res-dtype
                      (reduce casting/widest-datatype
                              (map dtype-base/elemwise-datatype args)))
        input-types (set (map argtypes/arg-type args))
        op-space (casting/simple-operation-space res-dtype)
        cast-fn (case op-space
                  :boolean #(if % (BooleanConversions/from %) false)
                  :int64 #(if % (NumericConversions/longCast %) Long/MIN_VALUE)
                  :float64 #(if % (NumericConversions/doubleCast %) Double/NaN)
                  (get @casting/*cast-table* res-dtype identity))]
    (cond
      (= input-types #{:scalar})
      (cast-fn (apply map-fn args))
      (input-types :iterable)
      (apply dispatch/typed-map (fn [& args]
                                  (cast-fn (apply map-fn args)))
             res-dtype args)
      :else
      (let [shapes (->> args
                        (map
                         #(when (dispatch/reader-like? (argtypes/arg-type %))
                            (dtype-base/shape %)))
                        (remove nil?))]
        (errors/when-not-errorf
         (apply = shapes)
         "emap - shapes don't match: %s"
         (vec shapes))
        (cond-> (emap-reader map-fn res-dtype cast-fn shapes args)
          (input-types :tensor)
          (dtype-base/reshape (first shapes)))))))
