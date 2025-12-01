(ns tech.v3.tensor.color-gradients
  "Implement a mapping from double->color for each entry in a tensor.  Produces an image
  of the same dimensions in pixels as the input tensor.

  Default color schemes are found here:
  https://reference.wolfram.com/language/guide/ColorSchemes.html"
  (:require [clojure.java.io :as io]
            [tech.v3.tensor :as dtt]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.unary-pred :as dup]
            [clojure.edn :as edn]
            [tech.v3.parallel.for :as pfor]
            [tech.v3.libs.buffered-image :as bufimg])
  (:import [tech.v3.datatype NDBuffer]
           [clojure.lang IFn]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(def gradient-map (delay (-> (io/resource "gradients.edn")
                             (slurp)
                             (edn/read-string))))
(def gradient-tens (delay (-> (io/resource "gradients.png")
                              (bufimg/load)
                              (dtt/ensure-tensor))))


(defn- flp-close
  [val desired & [error]]
  (< (Math/abs (- (double val) (double desired)))
     (double (or error 0.001))))


(defn gradient-name->gradient-line
  [gradient-name invert-gradient? gradient-default-n]
  (let [gradient-default-n (long gradient-default-n)
        gradient-line
        (cond
          (keyword? gradient-name)
          (let [src-gradient-info (get @gradient-map gradient-name)
                _ (when-not src-gradient-info
                    (throw (Exception.
                            (format "Failed to find gradient %s"
                                    gradient-name))))
                gradient-tens @gradient-tens]
            (dtt/select gradient-tens (:tensor-index src-gradient-info)))
          (dtt/tensor? gradient-name)
          gradient-name
          (instance? IFn gradient-name)
          (dtt/->tensor
           (->> (range gradient-default-n)
                (map (fn [idx]
                       (let [p-val (/ (double idx)
                                      (double gradient-default-n))
                             grad-val (gradient-name p-val)]
                         (when-not (= 3 (count grad-val))
                           (throw (Exception. (format
                                               "Gradient fns must return bgr tuples:
function returned: %s"
                                               grad-val))))
                         grad-val))))))
        n-pixels (long (first (dtype/shape gradient-line)))]
    ;;Gradients are accessed potentially many many times so reversing it here
    ;;is often wise as opposed to inline reversing in the main loops.
    (-> (if invert-gradient?
          (-> (dtt/select gradient-line (range (dec n-pixels) -1 -1)
                            :all)
              (dtype/copy! (dtt/reshape
                            (dtype/make-container :uint8
                                                  (dtype/ecount gradient-line))
                            [n-pixels 3])))
          gradient-line)
        (dtt/ensure-tensor))))


(defn colorize
  "Apply a color gradient to a tensor returning an image.  Takes A 1 or 2d tensor.
   If data-min, data-max aren't provided they are found in the data.
   A buffered image is returned.


  **10.000 - Note that this function now takes an option map as opposed to a variable number of option
  arguments.**

  src-tens - Source tensor whose shape determines the shape of the final image.

  gradient-name -  may be a keyword, in which it must be a key in @gradient-map and
      these gradients come from:
      https://reference.wolfram.com/language/guide/ColorSchemes.html.
    gradient-name may be a tensor of dimensions [n 3].
    gradient-name may be a function that takes a value from 0-1 and returns a tuple
    of length 3.

  Additional arguments:
  :data-min :data-max - If provided then the data isn't scanned for min and max.  If min
    is equal to 0 and max is equal to 1.0 then the data doesn't need to be normalized.
    data ranges are clamped to min and max.
  :alpha? - If true, an image with an alpha channel is returned.  This is useful for
    when your data has NAN or INFs as in that case the returned image is transparent
    in those sections.
  :check-invalid? - If true then the data is scanned for NAN or INF's.  Used in
    conjunction with :alpha?
  :invert-gradient? - When true, reverses the provided gradient.
  :gradient-default-n - When an IFn is provided, it is quantized over n steps.


  Current built-in gradients are:
  #{:neon-colors :aquamarine :coffee-tones :deep-sea-colors :fuchsia-tones :green-red
  :red-blue-tones :brass-tones :army-colors :green-pink-tones :green-brown-terrain
  :thermometer-colors :cherry-tones :atlantic-colors :dark-terrain :fall-colors
  :rainbow :fruit-punch-colors :solar-colors :dark-bands :avocado-colors
  :light-temperature-map :sunset-colors :candy-colors :watermelon-colors
  :beach-colors :island-colors :pastel :gray-yellow-tones :temperature-map
  :blue-green-yellow :bright-bands :sandy-terrain :gray-tones :rust-tones
  :pearl-colors :dark-rainbow :valentine-tones :cmyk-colors :mint-colors
  :lake-colors :pigeon-tones :aurora-colors :plum-colors :alpine-colors
  :light-terrain :southwest-colors :sienna-tones :brown-cyan-tones :rose-colors
  :starry-night-colors}"



  [src-tens gradient-name & {:keys [data-min data-max
                                    alpha?
                                    check-invalid?
                                    invert-gradient?
                                    gradient-default-n]
                             :or {gradient-default-n 200}}]
  (let [src-tens (dtt/ensure-tensor src-tens)
        img-shape (dtype/shape src-tens)
        {data-min :min
         data-max :max
         valid-indexes :valid-indexes
         src-reader :src-reader}
        (if (and data-min data-max)
          {:min data-min
           :max data-max
           :src-reader src-tens}
          ;;If we have to min/max check then we have to filter out invalid indexes.
          ;;In that case we prefilter the data and trim it.  We always check for
          ;;invalid data in the main loops below and this is faster than
          ;;pre-checking and indexing.
          (let [valid-indexes
                (when check-invalid?
                  (let [p (get dup/builtin-ops :tech.numerics/finite?)]
                    (argops/argfilter p src-tens)))
                valid-indexes
                (when (and valid-indexes
                           (not= (dtype/ecount valid-indexes)
                                 (dtype/ecount src-tens)))
                  valid-indexes)
               src-reader (if valid-indexes
                            (dtype/indexed-buffer valid-indexes src-tens)
                            src-tens)]
            (merge
             (dfn/descriptive-statistics src-reader [:min :max] {:nan-strategy :keep})
             {:src-reader src-reader
              :valid-indexes valid-indexes})))
        n-pixels (if valid-indexes
                   (dtype/ecount valid-indexes)
                   (dtype/ecount src-reader))
        data-min (double data-min)
        data-max (double data-max)
        _ (when (or (dfn/infinite? data-min)
                    (dfn/infinite? data-max))
            (throw (Exception. "NAN or INF in src data detected!")))
        data-range (- data-max data-min)
        src-reader (if-not (and (flp-close 0.0 data-min)
                                (flp-close 1.0 data-max))
                     (dtype/emap (fn [^double x]
                                   (-> (- x data-min)
                                       (/ data-range)))
                                      :float64
                                      src-reader)
                     src-reader)
        src-reader (dtype/->reader src-reader)
        img-type (if alpha?
                   :byte-abgr
                   :byte-bgr)
        res-image (case (count img-shape)
                    2 (bufimg/new-image (first img-shape) (second img-shape) img-type)
                    1 (bufimg/new-image 1 (first img-shape) img-type))
        ;;Flatten out src-tens and res-tens and make them readers
        n-channels (long (if alpha? 4 3))
        ^NDBuffer res-tens (dtt/reshape res-image [n-pixels n-channels])
        ^NDBuffer gradient-line (gradient-name->gradient-line
                                      gradient-name invert-gradient?
                                      gradient-default-n)
        n-gradient-increments (long (first (dtype/shape gradient-line)))
        line-last-idx (double (dec n-gradient-increments))
        n-pixels (long (if valid-indexes
                         (dtype/ecount valid-indexes)
                         n-pixels))]
    (if alpha?
      (pfor/parallel-for
       idx
       n-pixels
       (let [src-val (.readDouble src-reader idx)]
         (when (Double/isFinite src-val)
           (let [p-value (min 1.0 (max 0.0 src-val))
                 line-idx (long (Math/round (* p-value line-last-idx)))]
             ;;alpha channel first
             (.ndWriteLong res-tens idx 0 255)
             (.ndWriteLong res-tens idx 1 (.ndReadLong gradient-line line-idx 0))
             (.ndWriteLong res-tens idx 2 (.ndReadLong gradient-line line-idx 1))
             (.ndWriteLong res-tens idx 3 (.ndReadLong gradient-line line-idx 2))))))
      (pfor/parallel-for
       idx
       n-pixels
       (let [src-val (.readDouble src-reader idx)]
         (when (Double/isFinite src-val)
           (let [p-value (min 1.0 (max 0.0 src-val))
                 line-idx (long (Math/round (* p-value line-last-idx)))]
             (.ndWriteLong res-tens idx 0 (.ndReadLong gradient-line line-idx 0))
             (.ndWriteLong res-tens idx 1 (.ndReadLong gradient-line line-idx 1))
             (.ndWriteLong res-tens idx 2 (.ndReadLong gradient-line line-idx 2)))))))
    res-image))


(defn colorize->clj
  "Same as colorize but returns a ND sequence of [b g r] persistent vectors.
  For options, see documentation in colorize."
  [src-tens gradient-name & {:as options}]
  (when (seq src-tens)
    (let [src-dims (dtype/shape src-tens)]
      (-> (colorize src-tens gradient-name options)
          ;;In case of 1d.  colorize always returns buffered image which is always
          ;;2d.
          (dtt/reshape (concat src-dims [3]))
          (dtt/->jvm)))))
