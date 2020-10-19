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
           [tech.v3.datatype BooleanReader LongReader DoubleReader ObjectReader]))



(defn- emap-reader
  [map-fn res-dtype cast-fn shapes args]
  (let [n-elems (long (dtype-base/ecount (first args)))
        ^List args (mapv #(argops/ensure-reader % n-elems) args)
        argcount (.size args)]
    (if (= res-dtype :boolean)
      (case argcount
        1 (unary-pred/reader map-fn (.get args 0))
        2 (binary-pred/reader map-fn (.get args 0) (.get args 1))
        (reify BooleanReader
          (lsize [rdr] n-elems)
          (readBoolean [rdr idx] (boolean (apply map-fn
                                                 (map #(% idx) args))))))
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
                  (long (map-fn (arg idx))))))
          2 (let [lhs (first args)
                  rhs (second args)]
              (reify LongReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readLong [rdr idx]
                  (long (map-fn (lhs idx) (rhs idx))))))
          ;;default
          (reify LongReader
            (elemwiseDatatype [rdr] res-dtype)
            (lsize [rdr] n-elems)
            (readLong [rdr idx]
              (long (apply map-fn (map #(% idx) args))))))
        (casting/float-type? res-dtype)
        (case argcount
          1 (let [arg (first args)]
              (reify DoubleReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readDouble [rdr idx]
                  (double (map-fn (arg idx))))))
          2 (let [lhs (first args)
                  rhs (second args)]
              (reify DoubleReader
                (elemwiseDatatype [rdr] res-dtype)
                (lsize [rdr] n-elems)
                (readDouble [rdr idx]
                  (double (map-fn (lhs idx) (rhs idx))))))
          (reify DoubleReader
            (elemwiseDatatype [rdr] res-dtype)
            (lsize [rdr] n-elems)
            (readDouble [rdr idx]
              (double (apply map-fn (map #(% idx) args))))))
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
        cast-fn (get @casting/*cast-table* res-dtype identity)]
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
