(ns tech.v3.datatype.emap
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred])
  (:import [java.util List]
           [tech.v3.datatype BooleanReader LongReader DoubleReader ObjectReader]))



(defn- emap-reader
  [map-fn res-dtype shapes args]
  (let [n-elems (long (apply * (first shapes)))
        ^List args (mapv #(dispatch/scalar->reader % n-elems) args)
        argcount (.size args)]
    (if (= res-dtype :boolean)
      (case argcount
        1 (unary-pred/reader map-fn (.get args 0))
        2 (binary-pred/reader map-fn (.get args 0) (.get args 1))
        (reify BooleanReader
          (lsize [rdr] n-elems)
          (readBoolean [rdr idx] (boolean (apply map-fn
                                                 (map #(% idx) args))))))
      (case argcount
        1 (unary-op/reader map-fn res-dtype (.get args 0))
        2 (binary-op/reader map-fn res-dtype (.get args 0) (.get args 1))
        (cond
          (casting/integer-type? res-dtype)
          (reify LongReader
            (lsize [rdr] n-elems)
            (readLong [rdr idx]
              (long (apply map-fn (map #(% idx) args)))))
          (casting/float-type? res-dtype)
          (reify DoubleReader
            (lsize [rdr] n-elems)
            (readDouble [rdr idx]
              (double (apply map-fn (map #(% idx) args)))))
          :else
          (reify ObjectReader
            (lsize [rdr] n-elems)
            (readObject [rdr idx]
              (apply map-fn (map #(% idx) args)))))))))


(defn emap
  "Elemwise map
  1. If input are all scalars, results in a scalar.
  2. If any inputs are iterables, results in an iterable.
  3. Either a reader or a tensor is returned.  All input shapes
  have to match.
  res-dtype is nil it is deduced from unifying the argument datatypes"
  [map-fn res-dtype & args]
  (let [res-dtype (or res-dtype
                      (reduce casting/widest-datatype
                              (map dtype-base/elemwise-datatype args)))
        input-types (set (map argtypes/arg-type args))]
    (cond
      (= input-types #{:scalar})
      (apply map-fn args)
      (input-types :iterable)
      (apply dispatch/typed-map map-fn res-dtype args)
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
        (cond-> (emap-reader map-fn res-dtype shapes args)
          (input-types :tensor)
          (dtype-base/reshape (first shapes)))))))
