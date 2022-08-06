(ns tech.v3.datatype.gradient
  "Calculate the numeric gradient of successive elements.
  Contains simplified versions of numpy.gradient and numpy.diff."
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [com.github.ztellman.primitive-math :as pmath]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [tech.v3.datatype Buffer DoubleReader LongReader ObjectReader]))


(set! *unchecked-math* true)


(defn gradient1d
  "Implement gradient as `(f(x+1)-f(x-1))/2*dx`.
  At the boundaries use `(f(x)-f(x-1))/dx`.
  Returns a lazy representation, use clone to realize.

  Calculates the gradient inline returning a double reader.  For datetime types
  please convert to epoch-milliseconds first and the result will be in milliseconds.

  dx is a non-zero floating-point scalar and defaults to 1,
  representing the signed offset between successive sample points.

  Returns a double reader of the same length as data.

  Example:

```clojure
user> (require '[tech.v3.datatype.gradient :as dt-grad])
nil
user> (dt-grad/gradient1d [1 2 4 7 11 16])
[1.0 1.5 2.5 3.5 4.5 5.0]
user> (dt-grad/gradient1d [1 2 4 7 11 16] 2.)
[0.5 0.75 1.25 1.75 2.25 2.5]
```"
  ([data dx]
   (let [data (dt-base/->buffer data)
         dx (double dx)
         n-data (.lsize data)
         n-data-dec (dec n-data)]
     (reify DoubleReader
       (lsize [this] n-data)
       (readDouble [this idx]
         (let [prev-idx (pmath/max 0 (- idx 1))
               next-idx (pmath/min n-data-dec (+ idx 1))
               width (double (* dx (- next-idx prev-idx)))]
           (pmath// (- (.readDouble data next-idx)
                       (.readDouble data prev-idx))
                    width))))))
  ([data]
   (gradient1d data 1.)))


(defmacro ^:private append-diff
  [rtype n-elems read-fn cast-fn append reader]
  `(let [boundary# (dec ~n-elems)]
     (reify ~rtype
       (lsize [this] ~n-elems)
       (~read-fn [this idx#]
         (if (== boundary# idx#)
           (~cast-fn ~append)
           (let [next-idx# (unchecked-inc idx#)]
             (- (. ~reader ~read-fn next-idx#)
                (. ~reader ~read-fn idx#))))))))


(defmacro ^:private prepend-diff
  [rtype n-elems read-fn cast-fn append reader]
  `(let [boundary# 0]
     (reify ~rtype
       (lsize [this] ~n-elems)
       (~read-fn [this idx#]
         (if (== boundary# idx#)
           (~cast-fn ~append)
           (let [next-idx# idx#
                 idx# (unchecked-dec idx#)]
             (- (. ~reader ~read-fn next-idx#)
                (. ~reader ~read-fn idx#))))))))


(defmacro ^:private basic-diff
  [rtype n-elems read-fn reader]
  `(let [n-elems# (unchecked-dec ~n-elems)]
     (reify ~rtype
       (lsize [this] n-elems#)
       (~read-fn [this idx#]
        (let [next-idx# (unchecked-inc idx#)]
          (- (. ~reader ~read-fn next-idx#)
             (. ~reader ~read-fn idx#)))))))


(defn diff1d
  "Returns a lazy reader of each successive element minus the previous element of length
  input-len - 1 unless the `:prepend` option is used.

  Options:

  * `:prepend` - prepend a value to the returned sequence making it the same length
  as the passed in sequence.
  * `:append` - append a value to the end of the returned sequence making it the same
  length as the passsed in sequence.

  Examples:

```clojure
user> (dt-grad/diff1d [1 2 4 7 0])
[1 2 3 -7]
user> (dt-grad/diff1d (dt-grad/diff1d [1 2 4 7 0]))
[1 1 -10]
user> (dt-grad/diff1d [1 2 4] {:prepend 2})
[2 1 2]
user> (dt-grad/diff1d [1 2 4])
[1 2]
```"
  [data & [options]]
  (let [reader (dt-base/->buffer data)
        op-space (casting/simple-operation-space (.elemwiseDatatype reader))
        options (or options {})
        n-data (.lsize reader)
        addval (or (options :prepend) (options :append))]
    (errors/when-not-errorf
     (not (and (options :prepend) (options :append)))
     "prepend and append options cannot be used simultaneously")
    (cond
      (options :append)
      (case op-space
        :int64 (append-diff LongReader n-data readLong unchecked-long addval reader)
        :float64 (append-diff DoubleReader n-data readDouble unchecked-double addval reader)
        (append-diff ObjectReader n-data readObject identity addval reader))
      (options :prepend)
      (case op-space
        :int64 (prepend-diff LongReader n-data readLong unchecked-long addval reader)
        :float64 (prepend-diff DoubleReader n-data readDouble unchecked-double addval reader)
        (prepend-diff ObjectReader n-data readObject identity addval reader))
      :else
      (case op-space
        :int64 (basic-diff LongReader n-data readLong reader)
        :float64 (basic-diff DoubleReader n-data readDouble reader)
        (basic-diff ObjectReader n-data readObject reader)))))


(defn downsample
  "Downsample to n-elems using nearest algorithm.  If data is less than n-elems it is returned
  unchanged."
  (^Buffer [data ^long n-elems]
   (let [data (dt-base/->reader (or (dt-base/as-reader data) (vec data)))
         data-size (.lsize data)
         new-n (min n-elems data-size)
         multiple (/ (double (dec data-size)) (double (dec new-n)))]
     (case (casting/simple-operation-space (.elemwiseDatatype data))
       :int64 (reify LongReader
                (elemwiseDatatype [rdr] (.elemwiseDatatype data))
                (lsize [rdr] new-n)
                (readLong [rdr idx]
                  (.readLong data (Math/round (* multiple idx)))))
       :float64 (reify DoubleReader
                  (elemwiseDatatype [rdr] (.elemwiseDatatype data))
                  (lsize [rdr] new-n)
                  (readDouble [rdr idx]
                    (.readDouble data (Math/round (* multiple idx)))))
       (reify ObjectReader
         (elemwiseDatatype [rdr] (.elemwiseDatatype data))
         (lsize [rdr] new-n)
         (readObject [rdr idx]
           (.readObject data (Math/round (* multiple idx))))))))
  (^Buffer [data ^long n-elems window-fn]
   (let [data-size (dt-base/ecount data)
         n-elems (min n-elems data-size)
         window-size (/ (double data-size) (double n-elems))]
     (->
      (if (casting/numeric-type? (dt-base/elemwise-datatype data))
        (reify DoubleReader
          (lsize [rdr] n-elems)
          (readDouble [rdr idx]
            (let [start-idx (Math/round (* idx window-size))
                  end-idx (min data-size (+ start-idx (Math/round window-size)))]
              (double (window-fn (dt-base/sub-buffer data start-idx (- end-idx start-idx)))))))
        (reify ObjectReader
          (lsize [rdr] n-elems)
          (readObject [rdr idx]
            (let [start-idx (Math/round (* idx window-size))
                  end-idx (min data-size (+ start-idx (Math/round window-size)))]
              (window-fn (dt-base/sub-buffer data start-idx (- end-idx start-idx)))))))
      (dtype-proto/clone)))))


(comment
  (gradient1d [1 2 4 7 11 16])
  (gradient1d [1 2 4 7 11 16] 2)
  (diff1d [1 2 4 7 11 16])
  (diff1d [1 2 4 7 0])

  )
