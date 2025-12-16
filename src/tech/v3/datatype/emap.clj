(ns tech.v3.datatype.emap
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy-make-container :as copy-cmc]
            [tech.v3.datatype.dispatch :as dispatch]
            [tech.v3.datatype.op-dispatch :as op-dispatch]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1
                                               vectorized-dispatch-2]
             :as dispatch]
            [tech.v3.datatype.const-reader :refer [const-reader]]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.language :refer [cond]]
            [ham-fisted.api :as hamf]
            [ham-fisted.print :refer [implement-tostring-print]])
  (:import [java.util List]
           [tech.v3.datatype BooleanReader LongReader DoubleReader ObjectReader Buffer]
           [ham_fisted Casts])
  (:refer-clojure :exclude [cond]))


(defmacro ^:private cast-or-missing
  [expr cast-fn missing-val]
  `(if-let [retval# ~expr]
     (~cast-fn retval#)
     ~missing-val))


(defn- op-space->cast-fn
  [op-space res-dtype]
  (case op-space
    :boolean #(if % (Casts/booleanCast %) false)
    :int64 #(if % (Casts/longCast %) Long/MIN_VALUE)
    :float64 #(if % (Casts/doubleCast %) Double/NaN)
    (get @casting/*cast-table* res-dtype identity)))

(declare emap-reader)

(deftype LReader [^clojure.lang.IFn$LL read-fn ^long n-elems reader-dtype map-fn cast-fn readers]
  dtype-proto/POperationalElemwiseDatatype (operational-elemwise-datatype [this] reader-dtype)
  LongReader
  (elemwiseDatatype [this] reader-dtype)
  (lsize [this] n-elems)
  (readLong [this idx] (.invokePrim read-fn idx))
  (subBuffer [this sidx eidx]
    (emap-reader map-fn reader-dtype cast-fn readers (hamf/repeat (count readers) :reader))))

(defn- long-reader
  [read-fn n-elems reader-dtype map-fn cast-fn readers]
  (LReader. read-fn n-elems reader-dtype map-fn cast-fn readers))

(deftype DReader [^clojure.lang.IFn$LD read-fn ^long n-elems reader-dtype map-fn cast-fn readers]
  dtype-proto/POperationalElemwiseDatatype (operational-elemwise-datatype [this] reader-dtype)
  DoubleReader
  (elemwiseDatatype [this] reader-dtype)
  (lsize [this] n-elems)
  (readDouble [this idx] (.invokePrim read-fn idx))
  (subBuffer [this sidx eidx]
    (emap-reader map-fn reader-dtype cast-fn readers (hamf/repeat (count readers) :reader))))

(defn- double-reader
  [read-fn n-elems reader-dtype map-fn cast-fn readers]
  (DReader. read-fn n-elems reader-dtype map-fn cast-fn readers))

(deftype OReader [^clojure.lang.IFn$LO read-fn ^long n-elems reader-dtype map-fn cast-fn readers]
  dtype-proto/POperationalElemwiseDatatype (operational-elemwise-datatype [this] reader-dtype)
  ObjectReader
  (elemwiseDatatype [this] reader-dtype)
  (lsize [this] n-elems)
  (readObject [this idx] (.invokePrim read-fn idx))
  (subBuffer [this sidx eidx]
    (emap-reader map-fn reader-dtype cast-fn readers (hamf/repeat (count readers) :reader))))

(defn- object-reader
  [read-fn n-elems reader-dtype map-fn cast-fn readers]
  (OReader. read-fn n-elems reader-dtype map-fn cast-fn readers))

(defmacro ^:private read-buffers
  [n-buffers]
  `(~'map-fn ~@(->> (range n-buffers)
                    (map (fn [buf-idx]
                           `(.readObject ~(with-meta (symbol (str (char (+ (int \a) buf-idx))))
                                            {:tag 'Buffer})
                                         ~'idx))))))

(defn- emap-reader
  [map-fn output-space res-dtype cast-fn args arg-types]
  (let [readers (mapv (fn [arg arg-type]
                        (if-not (identical? arg-type :scalar)
                          (dtype-base/->reader arg output-space)
                          arg))
                      args arg-types)
        n-elems (long (reduce #(min ^long %1 ^long %2)
                              Long/MAX_VALUE
                              (lznc/map (fn ^long [arg]
                                          (if (instance? Buffer arg)
                                            (.lsize ^Buffer arg)
                                            Long/MAX_VALUE)) readers)))
        readers (mapv (fn [arg arg-type]
                        (if (identical? :scalar arg-type)
                          (const-reader (cast-fn arg) n-elems)
                          arg))
                      readers arg-types)
        n-readers (count readers)
        reader-fn
        (case n-readers
          3 (let [[a b c] readers]
              (cond
                (identical? output-space :int64)
                (fn ^long [^long idx] (Casts/longCast (read-buffers 3)))
                (identical? output-space :float64)
                (fn ^double [^long idx] (Casts/doubleCast (read-buffers 3)))
                :else
                (fn [^long idx]  (read-buffers 3))))
          4 (let [[a b c d] readers]
              (cond
                (identical? output-space :int64)
                (fn ^long [^long idx] (Casts/longCast (read-buffers 4)))
                (identical? output-space :float64)
                (fn ^double [^long idx] (Casts/doubleCast (read-buffers 4)))
                :else
                (fn [^long idx]  (read-buffers 4))))
          (let [^clojure.lang.IFn$LO apply-map-fn
                (fn [^long idx]
                  (apply map-fn (lznc/map #(.readObject ^Buffer % idx) readers)))]
            (cond
              (identical? output-space :int64)
              (fn ^long [^long idx] (Casts/longCast (.invokePrim apply-map-fn idx)))
              (identical? output-space :float64)
              (fn ^double [^long idx] (Casts/doubleCast (.invokePrim apply-map-fn idx)))
              :else
              (fn [^long idx]  (.invokePrim apply-map-fn idx)))))]
    (cond
      (instance? clojure.lang.IFn$LL reader-fn)
      (long-reader reader-fn n-elems res-dtype map-fn cast-fn readers)
      (instance? clojure.lang.IFn$LD reader-fn)
      (double-reader reader-fn n-elems res-dtype map-fn cast-fn readers)
      :else
      (object-reader reader-fn n-elems res-dtype map-fn cast-fn readers))))

(defn primitive-return-type
  [f]
  (let [rv (hamf-proto/returned-datatype f)]
    (if (or (identical? rv :int64) (identical? rv :float64))
      rv
      nil)))

(defn emap
  "Elemwise map:

  1. If input are all scalars, results in a scalar.
  2. If any inputs are iterables, results in an iterable.
  3. Either a reader or a tensor is returned.  All input shapes
     have to match.

  res-dtype is nil it is deduced from unifying the argument datatypes meaning
  map-fn, unless it is typed, is assumed to keep data in the same numeric space
  as the input."
  ([map-fn res-dtype x]
   (let [op (unary-op/->unary-operator map-fn)
         x-dt (casting/simple-operation-space (dtype-base/operational-elemwise-datatype x))
         output-space (or res-dtype (hamf-proto/returned-datatype op) x-dt)
         input-space (or (dtype-proto/input-datatype op) res-dtype)]
     (op-dispatch/dispatch-unary-op op input-space output-space x)))
  ([map-fn res-dtype x y]
   (let [op (binary-op/->binary-operator map-fn)]
     (if (nil? res-dtype)
       (op-dispatch/dispatch-binary-op op x y)
       (let [x-dt (casting/simple-operation-space (dtype-base/operational-elemwise-datatype x)
                                                  (dtype-base/operational-elemwise-datatype y))
             output-space (or res-dtype (primitive-return-type op) x-dt)
             input-space (or (dtype-proto/input-datatype op) x-dt)]
         (op-dispatch/dispatch-binary-op op input-space output-space x y)))))
  ([map-fn res-dtype x y & args]
   (let [args (hamf/concatv [x y] args)
         input-space (->> args
                          (lznc/map dtype-base/operational-elemwise-datatype)
                          (reduce casting/simple-operation-space))
         output-space (or res-dtype (hamf-proto/returned-datatype map-fn) input-space)
         res-dtype (or res-dtype output-space)
         arg-input-types (mapv argtypes/arg-type args)
         input-types (set arg-input-types)
         cast-fn (op-space->cast-fn input-space output-space)]
     (cond
       (= input-types #{:scalar})
       (cast-fn (apply map-fn args))
       (input-types :iterable)
       (op-dispatch/typed-map-n map-fn output-space res-dtype args arg-input-types)
       :else
       (let [shapes (->> args
                         (lznc/map
                          #(when (dispatch/reader-like? (argtypes/arg-type %))
                             (dtype-base/shape %)))
                         (lznc/remove nil?))]
         (errors/when-not-errorf
          (apply = shapes)
          "emap - shapes don't match: %s"
          (vec shapes))
         (cond-> (emap-reader map-fn output-space res-dtype cast-fn args arg-input-types)
           (input-types :tensor)
           (dtype-base/reshape (first shapes))))))))
