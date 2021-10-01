(ns tech.v3.tensor.dimensions.analytics
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.index-algebra :as idx-alg]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.list :as dtype-list]
            [tech.v3.tensor.dimensions.shape :as shape]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype Buffer PrimitiveList]
           [java.util List]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn any-offsets?
  [{:keys [shape]}]
  (boolean (some idx-alg/offset? shape)))


(defn shape-ary->strides
  "Strides assuming everything is increasing and packed."
  ^List [shape-vec]
  (let [retval (long-array (count shape-vec))
        shape-vec (dtype-base/->buffer shape-vec)
        n-elems (alength retval)
        n-elems-dec (dec n-elems)]
    (loop [idx n-elems-dec
           last-stride 1]
      (if (>= idx 0)
        (let [next-stride (* last-stride
                             (if (== idx n-elems-dec)
                               1
                               (.readLong shape-vec (inc idx))))]
          (aset retval idx next-stride)
          (recur (dec idx) next-stride))
        (dtype-base/->buffer retval)))))


(defn find-breaks
  "Attempt to reduce the dimensionality of the shape but do not
  change its elementwise global->local definition.  This is done because
  the running time of elementwise global->local is heavily dependent upon the
  number of dimensions in the shape."
  ([^List shape ^List strides ^List shape-ecounts]
   (let [n-elems (count shape)
         n-elems-dec (dec n-elems)]
     (loop [idx n-elems-dec
            last-stride 1
            last-break n-elems
            breaks nil]
       (if (>= idx 0)
         (let [last-shape-entry (if (== idx n-elems-dec)
                                  nil
                                  (.get shape (inc idx)))
               last-entry-bcast? (when last-shape-entry
                                   (idx-alg/broadcast? last-shape-entry))
               last-entry-number? (when last-shape-entry
                                    (or (number? last-shape-entry)
                                        (number? (idx-alg/get-reader
                                                  last-shape-entry))))
               next-stride (pmath/* last-stride
                                    (if (== idx n-elems-dec)
                                      1
                                      (if last-entry-number?
                                        (long (idx-alg/get-reader last-shape-entry))
                                        -1)))
               shape-entry (.get shape idx)
               shape-entry-number? (or (number? shape-entry)
                                       (number? (idx-alg/get-reader shape-entry)))
               current-offset? (idx-alg/offset? shape-entry)
               last-offset? (if last-shape-entry
                              (idx-alg/offset? last-shape-entry)
                              current-offset?)
               stride-val (long (.get strides idx))
               next-break
               (if (and (pmath/== stride-val next-stride)
                        shape-entry-number?
                        (and last-entry-number? (not last-entry-bcast?))
                        (not current-offset?)
                        (not last-offset?))
                 last-break
                 (inc idx))
               breaks (if (== next-break last-break)
                        breaks
                        (conj breaks (range next-break last-break)))]
           (recur (dec idx) stride-val next-break breaks))
         (conj breaks (range 0 last-break))))))
  ([{:keys [shape strides shape-ecounts]}]
   (find-breaks shape strides shape-ecounts)))


(defn- ensure-number-or-reader
  [item]
  (let [item (idx-alg/get-reader item)]
    (if (number? item)
      item
      (dtype-base/->reader item))))


(defn reduce-dimensionality
  "Make a smaller equivalent shape in terms of row-major addressing
  from the given shape."
  ([{:keys [shape strides
            shape-ecounts
            shape-ecount-strides]}
    offsets?]
   ;;Make sure shape only contains long objects as numbers
   (if (== 1 (count shape))
     {:shape [(ensure-number-or-reader (.get ^List shape 0))]
      :strides strides
      :offsets (mapv idx-alg/get-offset shape)
      :shape-ecounts shape-ecounts
      :shape-ecount-strides (or shape-ecount-strides
                                (shape-ary->strides shape-ecounts))}
     (let [^List processed-shape (mapv ensure-number-or-reader shape)
           ^List strides strides
           ^List shape-ecounts shape-ecounts
           breaks (find-breaks shape strides shape-ecounts)
           shape-ecounts (mapv
                          #(reduce * (map (fn [idx] (.get shape-ecounts (long idx)))
                                          %))
                          breaks)]
       {:shape (mapv #(if (== 1 (count %))
                        (.get processed-shape (first %))
                        (apply * (map (fn [idx]
                                        (.get processed-shape (long idx)))
                                      %)))
                     breaks)
        :strides (mapv
                  #(apply min Long/MAX_VALUE
                          (map (fn [idx]
                                 (.get strides (long idx)))
                               %))
                  breaks)
        :offsets (mapv
                  #(apply +
                          (map (fn [idx]
                                 (idx-alg/get-offset
                                  (.get ^List shape (long idx))))
                               %))
                  breaks)
        :shape-ecounts shape-ecounts
        :shape-ecount-strides (shape-ary->strides shape-ecounts)})))
  ([dims]
   (reduce-dimensionality dims (any-offsets? dims))))


(defn simple-direct-reduced-dims
  "Create the simplest case of reduced dimensions."
  [^long n-elems]
  {:shape [n-elems]
   :strides [1]
   :offsets [0]
   :shape-ecounts [n-elems]
   :shape-ecount-strides [1]})


(defn dims->reduced-dims
  [dims]
  (or (:reduced-dims dims) (reduce-dimensionality dims)))


(defn are-reduced-dims-bcast?
  "Fast version of broadcasting check for when we know the types
  the types of shapes and max-shape"
  [{:keys [^List shape ^List shape-ecounts] :as _reduced-dims}]
  (let [n-shape (count shape)
        broadcast? (loop [bcast? false
                          idx 0]
                     (if (and (not bcast?)
                              (< idx n-shape))
                       (recur (or bcast?
                                  (not= (shape/shape-entry->count (.get shape idx))
                                        (long (.get shape-ecounts idx))))
                              (inc idx))
                       bcast?))]
    broadcast?))


(defn are-dims-bcast?
  "Fast version of broadcasting check for when we know the types
  the types of shapes and max-shape.  Reduced dimensions may have a
  simplified definition of the shape element during broadcasting."
  [reduced-dims]
  (boolean (some idx-alg/broadcast? (:shape reduced-dims))))


(defn buffer-ecount
  "Return a minimum buffer element count of possible."
  [shape strides]
  (let [n-dims (count shape)
        ^List shape shape
        ^List strides strides]
    (loop [idx 0
           buffer-length (Long. 1)]
      (if (< idx n-dims)
        (let [new-shape-val (.get shape idx)
              stride-val (long (.get strides idx))
              [cmin cmax :as mmax]
              (cond
                (number? new-shape-val)
                [0 (dec (long new-shape-val))]
                (dtype-proto/has-constant-time-min-max? new-shape-val)
                [(long (dtype-proto/constant-time-min new-shape-val))
                 (long (dtype-proto/constant-time-max new-shape-val))])
              buffer-length (when (and buffer-length mmax)
                              (+ (long buffer-length)
                                 (* stride-val
                                    (- (long cmax) (long cmin)))))]
          (recur (unchecked-inc idx) buffer-length))
        buffer-length))))


(defn left-extend-shape
  [shape ^long n-new-shape]
  (if (== (count shape) n-new-shape)
    shape
    (vec (concat (repeat 1 (- n-new-shape (count shape)))
                 shape))))


(defn left-extend-strides
  [strides ^long n-new-strides]
  (if (== (count strides) n-new-strides)
    strides
    (vec (concat (repeat (first strides) (- n-new-strides (count strides)))
                 strides))))


(comment
  (reduce-dimensionality {:shape [4 4 4]
                          :strides [16 4 1]
                          :shape-ecounts [4 4 4]})
  )
