(ns tech.v3.datatype.copy-raw-to-item
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.argtypes :as argtypes]
            [tech.v3.datatype.copy :as dtype-cp]
            [tech.v3.parallel.for :as pfor]
            [ham-fisted.defprotocol :refer [extend extend-type extend-protocol]])
  (:import [ham_fisted Reductions])
  (:refer-clojure :exclude [extend extend-type extend-protocol]))



(defn raw-dtype-copy!
  [raw-data ary-target target-offset options]
  (let [elem-count (dtype-base/ecount raw-data)]
    (dtype-cmc/copy! raw-data
                     (dtype-base/sub-buffer ary-target target-offset elem-count)
                     options)
    [ary-target (+ (long target-offset) elem-count)]))


(extend-protocol dtype-proto/PCopyRawData
  Number
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (dtype-base/set-value! ary-target target-offset raw-data)
    [ary-target (+ target-offset 1)])
  Object
  (copy-raw->item! [raw-data ary-target ^long target-offset options]
    (cond
      (and (:rectangular? options) (dtype-base/array? raw-data))
      (let [dshape (dtype-base/shape raw-data)]
        (cond
          (== 1 (count dshape))
          (raw-dtype-copy! raw-data ary-target target-offset options)
          (< (long (last dshape)) 1000)
          ;;fast paths for rectangular arrays-of-arrays
          (raw-dtype-copy! (dtype-base/rectangular-nested-array->elemwise-reader
                            raw-data)
                           ary-target target-offset options)
          :else
          (let [ary-rdr (dtype-base/rectangular-nested-array->array-reader
                         raw-data)
                target-offset (long target-offset)
                n-sub-elems (long (last dshape))
                n-sub-arrays (.lsize ary-rdr)]
            (pfor/parallel-for
                ary-idx
                n-sub-arrays
                (dtype-cp/high-perf-copy!
                 (ary-rdr ary-idx)
                 (dtype-proto/sub-buffer ary-target (+ target-offset
                                                       (* n-sub-elems ary-idx))
                                         n-sub-elems)
                 n-sub-elems))
            [ary-target (+ target-offset (* n-sub-elems n-sub-arrays))])))
      (dtype-proto/convertible-to-reader? raw-data)
      (let [src-reader (dtype-base/->reader raw-data)]
        (if (or (not= :object (casting/flatten-datatype
                               (dtype-base/elemwise-datatype src-reader)))
                (and (not= 0 (dtype-base/ecount src-reader))
                     (= :scalar (argtypes/arg-type (src-reader 0)))))
          (raw-dtype-copy! src-reader ary-target target-offset options)
          (dtype-base/copy-raw-seq->item! (seq raw-data) ary-target target-offset options)))
      (instance? java.lang.Iterable raw-data)
      (dtype-base/copy-raw-seq->item! raw-data ary-target
                                      target-offset options)

      :else
      (do
        (dtype-base/set-value! ary-target target-offset raw-data)
        [ary-target (+ target-offset 1)]))))
