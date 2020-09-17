(ns tech.v3.tensor.dimensions
  "Compute tensors dimensions control the shape and stride of the tensor along with
  offsetting into the actual data buffer.  This allows multiple backends to share a
  single implementation of a system that will allow transpose, reshape, etc. assuming
  the backend correctly interprets the shape and stride of the dimension objects.

  Shape vectors may have an index buffer in them at a specific dimension instead of a
  number.  This means that that dimension should be indexed indirectly.  If a shape has
  any index buffers then it is considered an indirect shape."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.tensor.dimensions.select :as dims-select]
            [tech.v3.tensor.dimensions.analytics :as dims-analytics]
            [tech.v3.tensor.dimensions.shape :as shape]
            [tech.v3.tensor.dimensions.global-to-local :as gtol]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.index-algebra :as idx-alg]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors
             :refer [when-not-error]
             :as errors])
  (:import [tech.v3.datatype PrimitiveIO PrimitiveList ObjectReader
            LongReader LongNDReader]
           [java.util List Map]
           [clojure.lang IDeref]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(declare create-dimension-transforms)


(defrecord Dimensions [^List shape ;;list of things.  Not longs.
                       ^List strides ;;list of longs
                       ^long n-dims ;;(count shape)
                       ^List shape-ecounts ;;list of longs
                       ^List shape-ecount-strides ;;list of naive strides if the shape-ecounts were the shape.
                       ^long overall-ecount ;;ecount of the dimensions
                       ;;Not always detectable from the shape, if it is this is the smallest dense buffer
                       ;;that could contain data described with these dimensions.  May be nil or long.
                       buffer-ecount
                       ;;the result of the dims-analytics reduction process.
                       reduced-dims
                       ^boolean broadcast? ;; true any shape member is broadcast
                       ^boolean offsets? ;; true if any shape member has offsets
                       ^boolean shape-direct? ;; true if all shape entries are numbers
                       ;;True if all shape entries are numbers and based on those numbers
                       ;;the strides indicate data is packed.
                       ^boolean native?
                       ;;Are the strides ordered from greatest to least?
                       ^boolean access-increasing?
                       ;;delay of global->local transformation
                       global->local
                       ;;delay of global->local transformation
                       local->global]
  dtype-proto/PShape
  (shape [item] shape-ecounts)
  dtype-proto/PCountable
  (ecount [item] overall-ecount))


(defn dimensions
  "Dimensions contain information about how to map logical global indexes to local
  buffer addresses."
  ([shape strides shape-ecounts shape-ecount-strides]
   (let [n-dims (count shape)
         _ (when-not (== n-dims (count strides))
             (throw (Exception. "shape/strides ecount mismatch")))
         shape-direct? (every? idx-alg/direct? shape)
         offsets? (boolean (and (not shape-direct?)
                                (some idx-alg/offset? shape)))
         broadcast? (boolean (and (not shape-direct?)
                                  (some idx-alg/broadcast? shape)))
         access-increasing? (boolean (if (> n-dims 1)
                                       (apply >= strides)
                                       true))
         ^List strides strides
         ^List shape shape
         native? (boolean
                  (and shape-direct?
                       (== 1 (long (.get strides (dec n-dims))))
                       (or (== n-dims 1)
                           (loop [idx 1]
                             (let [ridx (- n-dims idx 1)]
                               (cond
                                 (== idx n-dims) true
                                 (and
                                  (not= (.get shape ridx) 1)
                                  (not= (long (.get strides ridx))
                                        (let [didx (unchecked-inc ridx)]
                                          (* (long (.get strides didx))
                                             (long (.get shape didx))))))
                                 false
                                 :else
                                 (recur (unchecked-inc idx))))))))
         overall-ecount (long (apply * shape-ecounts))
         reduced-dims (dims-analytics/reduce-dimensionality
                       {:shape shape
                        :strides strides
                        :shape-ecounts shape-ecounts})
         buffer-ecount (dims-analytics/buffer-ecount
                        (:shape reduced-dims)
                        (:strides reduced-dims))
         half-retval (->Dimensions shape
                                   strides
                                   n-dims
                                   shape-ecounts
                                   shape-ecount-strides
                                   overall-ecount
                                   buffer-ecount
                                   reduced-dims
                                   broadcast?
                                   offsets?
                                   shape-direct?
                                   native?
                                   access-increasing?
                                   nil
                                   nil)]
     (create-dimension-transforms half-retval)))
  ([shape strides]
   (let [shape-ecounts (mapv shape/shape-entry->count shape)
         shape-ecount-strides (dims-analytics/shape-ary->strides shape-ecounts)]
     (dimensions shape strides shape-ecounts shape-ecount-strides)))
  ([shape]
   (let [n-dims (count shape)
         ^List shape shape
         strides (dims-analytics/shape-ary->strides shape)
         shape-ecounts shape
         shape-ecount-strides strides
         shape-direct? true
         offsets? false
         broadcast? false
         native? true
         access-increasing? true
         overall-ecount (long (* (long (.get shape 0))
                                 (long (.get strides 0))))
         reduced-dims (dims-analytics/simple-direct-reduced-dims
                       overall-ecount)
         half-retval (->Dimensions shape
                                   strides
                                   n-dims
                                   shape-ecounts
                                   shape-ecount-strides
                                   overall-ecount
                                   overall-ecount
                                   reduced-dims
                                   broadcast?
                                   offsets?
                                   shape-direct?
                                   native?
                                   access-increasing?
                                   nil
                                   nil)]
     (create-dimension-transforms half-retval))))


(defn ecount
  ^long [{:keys [overall-ecount]}]
  (long overall-ecount))

(defn buffer-ecount
  "What is the necessary ecount for a given buffer.  Maybe  nil if this could not be detected
  from the dimension arguments."
  [{:keys [buffer-ecount]}]
  buffer-ecount)


(defn ->2d-shape
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [{:keys [shape]}]
  (shape/->2d shape))


(defn ->batch-shape
  "Given dimensions, return new dimensions with the lowest (fastest-changing) dimension
  unchanged and the rest of the dimensions multiplied into the higher dimension."
  [{:keys [shape]}]
  (shape/->2d shape))


(defn shape
  [{:keys [shape-ecounts]}]
  shape-ecounts)


(defn strides
  [{:keys [strides]}]
  strides)


(defn direct?
  [{:keys [shape-direct?]}]
  shape-direct?)


(defn native?
  [{:keys [native?]}]
  native?)


(defn dense?
  "This query isn't answered well above but if it is native then it is definitely
  dense."
  [{:keys [native?]}]
  native?)


(defn indirect?
  [dims]
  (not (direct? dims)))


(defn access-increasing?
  "Are these dimensions setup such a naive seq through the data will be accessing memory
  in order.  This is necessary for external library interfaces (blas, cudnn).  An
  example would be after any nontrivial transpose that is not made concrete (copied)
  this condition will not hold."
  [{:keys [access-increasing?]}]
  access-increasing?)


(defn ->most-rapidly-changing-dimension
  "Get the size of the most rapidly changing dimension"
  ^long [{:keys [shape-ecounts]}]
  (last shape-ecounts))


(defn ->least-rapidly-changing-dimension
  "Get the size of the least rapidly changing dimension"
  ^long [{:keys [shape-ecounts]}]
  (first shape-ecounts))


(defn local-address->local-shape
  "Shape and strides are not transposed.  Returns
  [valid? local-shape-as-list]"
  [shape offsets strides shape-mins addr]
  (let [strides (dtype-base/->reader strides)
        shape (dtype-base/->reader shape)
        offsets (dtype-base/->reader offsets)
        addr (int addr)
        n-elems (.lsize strides)
        ^PrimitiveList retval (dtype/make-container :list :int32 0)]
    (loop [idx 0
           addr addr]
      (if (< idx n-elems)
        (let [local-stride (.readLong strides idx)
              shape-idx (quot addr local-stride)
              local-shape (.readLong shape idx)]
          (if (and (< shape-idx local-shape)
                   (>= shape-idx (long (shape-mins idx))))
            (let [shape-idx (- shape-idx (.readLong offsets idx))
                  shape-idx (if (< shape-idx 0)
                              (+ shape-idx local-shape)
                              shape-idx)]
              (.addLong retval (int shape-idx))
              (recur (unchecked-inc idx) (rem addr local-stride)))
            (recur n-elems -1)))
        (when (= 0 addr)
          retval)))))


(defn ->global->local
  ^LongNDReader [dims]
  @(:global->local dims))


(defn ->local->global
  ^PrimitiveIO [dims]
  @(:local->global dims))


(defn get-elem-dims-local->global
  "Harder translation than above.  May return nil in the case where the inverse
  operation hasn't yet been derived.  In this case, the best you can do is a O(N)
  iteration similar to dense math."
  ^PrimitiveIO
  [dims global->local*]
  (let [dims-ecount (ecount dims)]
    (if (:direct? dims)
      (reify LongReader
        (lsize [rdr] dims-ecount)
        (readLong [item local-idx] local-idx))
      ;;TODO - rebuild faster and less memory intensive paths for this.
      ;;This will just make the problem go away and allows rapid indexing.
      (let [group-map (dfn/arggroup @global->local*)]
        (reify ObjectReader
          (lsize [rdr] (long (count group-map)))
          (readObject [item local-idx]
            (get group-map local-idx)))))))


(defn create-dimension-transforms [dims]
  (let [global->local (delay (gtol/dims->global->local dims))]
    (assoc dims
           :global->local global->local
           ;;:global->local (delay (get-elem-dims-global->local dims))
           :local->global (delay (get-elem-dims-local->global
                                  dims global->local)))))


(defn in-place-reshape
  "Return new dimensions that correspond to an in-place reshape.  This is a very
  difficult algorithm to get correct as it needs to take into account changing strides
  and dense vs non-dense dimensions."
  [existing-dims shape]
  (let [new-dims (dimensions shape)]
    (errors/when-not-errorf (<= (ecount new-dims)
                                (ecount existing-dims))
                            "Reshaped dimensions %s are larger than tensor %s"
                            (ecount new-dims) (ecount existing-dims))
    new-dims))


(defn transpose
  "Transpose the dimensions.  Returns a new dimensions that will access memory in a
  transposed order.
  Dimension 0 is the leftmost (greatest) dimension:

  (transpose tens (range (count (shape tens))))

  is the identity operation."
  [{:keys [^List shape ^List strides
           ^List shape-ecounts]
    :as dims} reorder-vec]
  (when-not-error (= (count (distinct reorder-vec))
                     (count shape))
    "Every dimension must be represented in the reorder vector")
  (let [shape (mapv #(.get shape (int %)) reorder-vec)
        strides (mapv #(.get strides (int %)) reorder-vec)
        shape-ecounts (mapv #(.get shape-ecounts (int %)) reorder-vec)
        shape-ecount-strides
        (dims-analytics/shape-ary->strides shape-ecounts)]
    (dimensions shape strides shape-ecounts shape-ecount-strides)))



(defn select
  "Expanded implementation of the core.matrix select function call.  Each dimension must
  have an entry and each entry may be:
:all (identity)
:lla (reverse)
persistent-vector: [0 1 2 3 4 4 5] (not supported by all backends)
map: {:type [:+ :-]
      :min-item 0
      :max-item 50}
  Monotonically increasing/decreasing bounded (inclusive) sequences

tensor : int32, dense vector only.  Not supported by all backends.

;;Some examples
https://cloojure.github.io/doc/core.matrix/clojure.core.matrix.html#var-select"
  [dims & args]
  (let [data-shp (shape dims)]
    (errors/when-not-errorf (= (count data-shp)
                               (count args))
                            "arg count (%d) must match shape count (%d)"
                            (count args) (count data-shp))
    (let [{shape :shape
           strides :strides
           offset :offset}
          (dims-select/select args (:shape dims) (:strides dims))]
      (assoc (dimensions shape strides)
             :elem-offset offset))))


(defn rotate
  "Dimensional rotations are applied via offsetting."
  [{:keys [shape] :as dims} new-offset-vec]
  (when-not (== (count shape) (count new-offset-vec))
    (throw (Exception. "Offset vec must be same length as shape.")))
  (if (every? #(= 0 (long %)) new-offset-vec)
    dims
    (dimensions (mapv (fn [old-shape offset-val]
                        (idx-alg/offset old-shape offset-val))
                      (:shape dims)
                      new-offset-vec)
                (:strides dims))))


(defn broadcast
  "Broadcast one or more dimensions. End result is shape of target matches
  new-shape"
  [{:keys [shape shape-ecounts strides] :as dims} new-shape]
  (if (= shape-ecounts (vec new-shape))
    dims
    (let [shape (dims-analytics/left-extend-shape shape (count new-shape))
          strides (dims-analytics/left-extend-strides strides (count new-shape))]
      (dimensions
       (mapv idx-alg/broadcast shape new-shape)
       strides))))


(defn slice
  "Slice off the leftmost n-elems dimensions.
  Return a sub-dim object that doesn't change and a long reader of offsets.
  Returns
  {:dimension - dimensions for every sub object
   :offsets - long reader of offsets for the buffer.
  }"
  [{:keys [shape strides] :as original-dims} n-elems]
  (let [n-elems (long n-elems)
        n-shape (count shape)]
    (if (and (== n-elems 1)
             (== n-shape 1))
      ;;base case where there is only one dimension.  This really means just turn
      ;;the tensor into a reader.
      {:dimensions original-dims
       :offsets (long-array [0])}
      (let [sub-dims (dimensions (vec (drop n-elems shape))
                                 (vec (drop n-elems strides)))
            offset-dims (dimensions (vec (take n-elems shape))
                                    (vec (take n-elems strides)))]
        {:dimensions sub-dims
         :offsets (->global->local offset-dims)}))))


(defn slice-right
  "Slice off the rightmost n-elems dimensions.
  Return a sub-dim object that doesn't change and a long reader of offsets.
  Returns
  {:dimension - dimensions for every sub object
   :offsets - long reader of offsets for the buffer.
  }"
  [{:keys [shape strides] :as original-dims} n-elems]
  (let [n-elems (long n-elems)
        n-shape (count shape)]
    (if (== n-elems n-shape)
      ;;base case where there is only one dimension.  This really means just turn
      ;;the tensor into a reader.
      {:dimensions original-dims
       :offsets (dtype/->reader (long-array [0]))}
      (let [offset-dims (dimensions (vec (take-last n-elems shape))
                                    (vec (take-last n-elems strides)))
            n-sub-dims (- n-shape n-elems)
            sub-dims (dimensions (vec (take n-sub-dims shape))
                                 (vec (take n-sub-dims strides)))]
        {:dimensions sub-dims
         :offsets (->global->local offset-dims)}))))


(defn dimensions->column-stride
  ^long [{:keys [shape strides]}]
  (long
   (let [dim-count (count strides)]
     (if (> dim-count 1)
       ;;get the second from the last stride
       (get strides (- dim-count 2))
       ;;Get the dimension count
       (get shape 0 1)))))


(defn trans-2d-shape
  [trans-a? dims]
  (let [[rows cols] (->2d-shape dims)]
    (if trans-a?
      [cols rows]
      [rows cols])))


(defn matrix-column-stride
  "Returns the larger of the 2 strides"
  ^long [{:keys [shape strides]}]
  (errors/when-not-errorf (= 2 (count shape))
                          "Not a matrix: %s" shape)
  (apply max strides))


(defn matrix-element-stride
  ^long [{:keys [shape strides]}]
  (errors/when-not-errorf (= 2 (count shape))
                          "Not a matrix: %s" shape)
  (apply min strides))
