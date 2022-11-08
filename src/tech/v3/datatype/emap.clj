(ns tech.v3.datatype.emap
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op])
  (:import [java.util List]
           [tech.v3.datatype BooleanReader LongReader DoubleReader ObjectReader]
           [ham_fisted Casts]))


(defmacro ^:private cast-or-missing
  [expr cast-fn missing-val]
  `(if-let [retval# ~expr]
     (~cast-fn retval#)
     ~missing-val))


(defn- emap-reader
  [map-fn res-dtype cast-fn args]
  (let [n-elems (long (dtype-base/ecount (first args)))
        ^List args (mapv #(argops/ensure-reader % n-elems) args)
        argcount (.size args)]
    (case argcount
      1 (unary-op/reader map-fn res-dtype (args 0))
      2 (binary-op/reader map-fn res-dtype (args 0) (args 1))
      (case (casting/simple-operation-space res-dtype)
        :int64
        (reify LongReader
          (elemwiseDatatype [rdr] res-dtype)
          (lsize [rdr] n-elems)
          (readLong [rdr idx]
            (cast-or-missing (apply map-fn (map #(% idx) args))
                             Casts/longCast
                             Long/MIN_VALUE)))
        :float64
        (reify DoubleReader
          (elemwiseDatatype [rdr] res-dtype)
          (lsize [rdr] n-elems)
          (readDouble [rdr idx]
            (cast-or-missing (apply map-fn (map #(% idx) args))
                             Casts/doubleCast
                             Double/NaN)))
        (reify ObjectReader
          (elemwiseDatatype [rdr] res-dtype)
          (lsize [rdr] n-elems)
          (readObject [rdr idx]
            (cast-fn (apply map-fn (map #(% idx) args)))))))))


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
                  :boolean #(if % (Casts/booleanCast %) false)
                  :int64 #(if % (Casts/longCast %) Long/MIN_VALUE)
                  :float64 #(if % (Casts/doubleCast %) Double/NaN)
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
        (cond-> (emap-reader map-fn res-dtype cast-fn args)
          (input-types :tensor)
          (dtype-base/reshape (first shapes)))))))
