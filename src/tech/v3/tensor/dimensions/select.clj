(ns tech.v3.tensor.dimensions.select
  "Selecting subsets from a larger set of dimensions leads to its own algebra."
  (:require [tech.v3.datatype.index-algebra :as idx-alg]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.list :as dtype-list])
  (:import [tech.v3.datatype Buffer]
           [java.util ArrayList List]))

(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn apply-select-arg-to-dimension
  "Given a dimension and select argument, create a new dimension with
the selection applied."
  [dim select-arg]
  (if (= select-arg :all)
    dim
    (idx-alg/select dim select-arg)))


(defn select
  "Apply a select seq to a dimension.
  Return new shape, stride, offset array
  along with new buffer offset and if it can be calculated a new
  buffer length."
  [select-seq shape-vec stride-vec]
  (let [^List select-vec (vec select-seq)
        ^List shape-vec shape-vec
        ^List stride-vec stride-vec
        result-shape (ArrayList.)
        result-stride (dtype-list/make-list :int64)
        n-elems (.size select-vec)]
    (when-not (== (.size select-vec)
                  (.size shape-vec))
      (throw (Exception. "Shape,select vecs do not match")))
    (loop [idx 0
           buffer-offset 0]
      (if (< idx n-elems)
        (let [new-shape-val (apply-select-arg-to-dimension
                             (.get shape-vec idx)
                             (.get select-vec idx))
              ;;scalar means to evaluate the result immediately and do not
              ;;add to result dims.
              new-shape-scalar? (:select-scalar? (meta new-shape-val))
              stride-val (long (.get stride-vec idx))
              ;;This resets ranges to start at zero at the cost of causing our host
              ;;to offset the buffer.  In the case where the range is incrementing
              ;;by 1, however, this leads to a native mapping to underlying buffers.
              [cmin new-shape-val]
              (if (and (not (number? new-shape-val))
                       (dtype-proto/has-constant-time-min-max? new-shape-val))
                (let [cmin (long (dtype-proto/constant-time-min new-shape-val))]
                  [cmin (if (dtype-proto/convertible-to-range? new-shape-val)
                          (-> (dtype-proto/range-offset new-shape-val (- cmin))
                              (idx-alg/simplify-range->direct))
                          (dfn/- new-shape-val cmin))])
                [nil new-shape-val])
              buffer-offset (long (if cmin
                                    (+ buffer-offset
                                       (* stride-val (long cmin)))
                                    buffer-offset))]
          (when-not new-shape-scalar?
            (.add result-shape new-shape-val)
            (.addLong result-stride stride-val))
          (recur (unchecked-inc idx) buffer-offset))
        {:shape result-shape
         :strides result-stride
         :offset buffer-offset}))))
