(ns tech.v3.datatype.clj-range
  "Datatype bindings for clojure ranges."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [ham-fisted.defprotocol :refer [extend extend-type extend-protocol]])
  (:import [clojure.lang LongRange Range]
           [tech.v3.datatype LongReader DoubleReader]
           [ham_fisted Ranges$LongRange Ranges$DoubleRange]
           [java.lang.reflect Field])
  (:refer-clojure :exclude [extend extend-type extend-protocol]))


(set! *warn-on-reflection* true)


(def ^Field lr-step-field (doto (.getDeclaredField ^Class LongRange "step")
                            (.setAccessible true)))


(extend-type LongRange
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [rng] :int64)
  dtype-proto/PECount
  (ecount [rng] (.count rng))
  dtype-proto/PClone
  (clone [rng] rng)
  dtype-proto/PToBuffer
  (convertible-to-buffer? [rng] true)
  (->buffer [rng]
    (dtype-proto/->reader rng))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [rng] true)
  (constant-time-min [rng] (let [start (long (first rng))
                                 step (long (.get lr-step-field rng))
                                 n-elems (.count rng)]
                             (if (pos? step) start (+ start (* step (dec n-elems))))))
  (constant-time-max [rng] (let [start (long (first rng))
                                 step (long (.get lr-step-field rng))
                                 n-elems (.count rng)]
                             (if (pos? step)
                               (+ start (* step (dec n-elems)))
                               start)))
  dtype-proto/PToReader
  (convertible-to-reader? [rng] true)
  (->reader [rng]
    (let [start (long (first rng))
          step (long (.get ^Field lr-step-field rng))
          n-elems (.count rng)]
      (-> (Ranges$LongRange. start (+ start (* step n-elems)) step (meta rng))
          (dtype-proto/->reader)))))


(def r-step-field (doto (.getDeclaredField ^Class Range "step")
                    (.setAccessible true)))


(defmacro range-reader-macro
  [datatype rng _options]
  `(let [rng# ~rng
         start# (casting/datatype->cast-fn :unknown ~datatype (first rng#))
         step# (casting/datatype->cast-fn :unknown ~datatype
                                          (.get ^Field r-step-field rng#))
         n-elems# (.count rng#)]
      (reify ~(typecast/datatype->reader-type datatype)
        (lsize [rdr#] n-elems#)
        (read [rdr# idx#]
          (errors/check-idx idx# n-elems#)
          (casting/datatype->cast-fn :unknown ~datatype
                                     (-> (* step# idx#)
                                         (+ start#)))))))


(extend-type Range
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [rng] (dtype-proto/elemwise-datatype
                            (if (> (.count rng) 1)
                              (second rng)
                              (first rng))))
  dtype-proto/PECount
  (ecount [rng] (.count rng))
  dtype-proto/PClone
  (clone [rng] rng)
  dtype-proto/PToBuffer
  (convertible-to-buffer? [rng] true)
  (->buffer [rng]
    (dtype-proto/->reader rng))
  dtype-proto/PConstantTimeMinMax
  (has-constant-time-min-max? [rng] true)
  (constant-time-min [rng] (let [start (long (first rng))
                                 step (long (.get lr-step-field rng))
                                 n-elems (.count rng)
                                 last-val (+ start (* step (dec n-elems)))]
                             (min start last-val)))
  (constant-time-max [rng] (let [start (first rng)
                                 step (.get lr-step-field rng)
                                 n-elems (.count rng)
                                 last-val (+ start (* step (dec n-elems)))]
                             (max start last-val)))
  dtype-proto/PToReader
  (convertible-to-reader? [rng] (contains?
                                 #{:int8 :int16 :int32 :int64
                                   :float32 :float64}
                                 (dtype-proto/elemwise-datatype rng)))
  (->reader [rng]
    (let [dtype  (dtype-proto/elemwise-datatype rng)]
      (if (casting/integer-type? dtype)
        (let [start (casting/datatype->cast-fn :unknown :int64 (first rng))
              step (casting/datatype->cast-fn :unknown :int64
                                              (.get ^Field r-step-field rng))
              n-elems (.count rng)
              last-val (+ start (* step (dec n-elems)))]
          (-> (Ranges$LongRange. start (+ start (* step n-elems)) step (meta rng))
              (dtype-proto/->reader)))
        (let [start (casting/datatype->cast-fn :unknown :float64 (first rng))
              step (casting/datatype->cast-fn :unknown :float64
                                              (.get ^Field r-step-field rng))
              n-elems (.count rng)
              last-val (+ start (* step (dec n-elems)))]
          (-> (Ranges$DoubleRange. start (+ start (* step n-elems)) step (meta rng))
              (dtype-proto/->reader)))))))
