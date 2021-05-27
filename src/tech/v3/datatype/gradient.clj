(ns tech.v3.datatype.gradient
  "Calculate the numeric gradient of successive elements.
  Contains simplified versions of numpy.gradient and numpy.diff."
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.casting :as casting]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype Buffer DoubleReader LongReader ObjectReader]))


(defn gradient1d
  "Implement gradient as `(f(x+h)-f(x-h))/2*h`.
  At the boundaries use `(f(x)-f(x-h))/h`.
  Returns a lazy representation, use clone to realize.

  Calculates the gradient inline returning a double reader.  For datetime types
  please convert to epoch-milliseconds first and the result will be in milliseconds.

  h is always a single integer scalar and defaults to 1.  Returns a double reader
  of the same length as data.

  Example:

```clojure
user> (require '[tech.v3.datatype.gradient :as dt-grad])
nil
user> (dt-grad/gradient1d [1 2 4 7 11 16])
[1.0 1.5 2.5 3.5 4.5 5.0]
user> (dt-grad/gradient1d [1 2 4 7 11 16] 2)
[0.5 0.75 1.25 1.75 2.25 2.5]
```"
  ([data h]
   (let [data (dt-base/->buffer data)
         h (long h)
         n-data (.lsize data)
         n-data-dec (dec n-data)]
     (reify DoubleReader
       (lsize [this] n-data)
       (readDouble [this idx]
         (let [prev-idx (pmath/max 0 (- idx 1))
               next-idx (pmath/min n-data-dec (+ idx 1))
               width (double (* h (- next-idx prev-idx)))]
           (pmath// (- (.readDouble data next-idx)
                       (.readDouble data prev-idx))
                    width))))))
  ([data]
   (gradient1d data 1)))


(defn diff1d
  "Returns a lazy reader of each successive element minus the previous element of length
  input-len - 1.

  Examples:

```clojure
user> (dt-grad/diff1d [1 2 4 7 0])
[1 2 3 -7]
user> (dt-grad/diff1d (dt-grad/diff1d [1 2 4 7 0]))
[1 1 -10]
```"
  [data]
  (let [reader (dt-base/->buffer data)
        n-data (.lsize reader)
        n-data-dec (dec n-data)
        n-result (max 0 n-data-dec)]
    (case (casting/simple-operation-space (.elemwiseDatatype reader))
      :int64 (reify LongReader
               (lsize [this] n-result)
               (readLong [this idx]
                 (let [next-idx (min n-data-dec (unchecked-inc idx))]
                   (pmath/- (.readLong reader next-idx)
                            (.readLong reader idx)))))
      :float64 (reify DoubleReader
                 (lsize [this] n-result)
                 (readDouble [this idx]
                   (let [next-idx (min n-data-dec (unchecked-inc idx))]
                     (pmath/- (.readDouble reader next-idx)
                              (.readDouble reader idx)))))
      (reify ObjectReader
        (lsize [this] n-result)
        (readObject [this idx]
          (let [next-idx (min n-data-dec (unchecked-inc idx))]
            ;;Specifically using clojure's number tower here.
            (- (.readObject reader next-idx)
               (.readObject reader idx))))))))


(comment
  (gradient1d [1 2 4 7 11 16])
  (gradient1d [1 2 4 7 11 16] 2)
  (diff1d [1 2 4 7 11 16])
  (diff1d [1 2 4 7 0])

  )
