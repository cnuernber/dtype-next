(ns tech.v3.tensor.dimensions.shape
  "A shape vector entry can be a number of things.  We want to be precise
  with handling them and abstract that handling so new things have a clear
  path."
  (:require [tech.v3.datatype.errors :refer [when-not-error]]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.index-algebra :as idx-alg]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn shape-entry->count
  "Return a vector of counts of each shape."
  ^long [shape-entry]
  (if (number? shape-entry)
    (long shape-entry)
    (dtype-base/ecount shape-entry)))


(defn shape->count-vec
  ^longs [shape-vec]
  (mapv shape-entry->count shape-vec))


(defn direct-shape?
  [shape]
  (every? idx-alg/direct? shape))


(defn indirect-shape?
  [shape]
  (boolean (some (complement idx-alg/direct?) shape)))


(defn ecount
  "Return the element count indicated by the dimension map"
  ^long [shape]
  (let [count-vec (shape->count-vec shape)
        n-items (count count-vec)]
    (loop [idx 0
           sum 1]
      (if (< idx n-items)
        (recur (unchecked-inc idx)
               (* (long (count-vec idx))
                  sum))
        sum))))


(defn- ensure-direct
  [shape-seq]
  (when-not-error (direct-shape? shape-seq)
    "Index buffers not supported for this operation.")
  shape-seq)


(defn ->2d
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [shape]
  (when-not-error (seq shape)
    "Invalid shape in dimension map")
  (if (= 1 (count shape))
    [1 (first shape)]
    [(apply * (ensure-direct (drop-last shape))) (last shape)]))


(defn ->batch
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [shape]
  (when-not-error (seq shape)
    "Invalid shape in dimension map")
  (if (= 1 (count shape))
    [1 (first shape)]
    [(first shape) (apply * (ensure-direct (drop 1 shape)))]))
