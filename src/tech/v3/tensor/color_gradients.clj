(ns tech.v3.tensor.color-gradients
  "https://reference.wolfram.com/language/guide/ColorSchemes.html"
  (:require [clojure.java.io :as io]
            [tech.v3.tensor :as dtt]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.unary-op :as unary-op]
            [clojure.edn :as edn]
            [tech.v3.parallel.for :as pfor]
            [tech.libs.buffered-image :as bufimg])
  (:import [java.awt.image BufferedImage]
           [tech.v3.datatype PrimitiveIO]
           [clojure.lang IFn]))


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
            (dtt/select gradient-tens
                        (:tensor-index src-gradient-info)
                        :all :all))
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
              (dtype/copy! (dtt/reshape (dtype/make-container :typed-buffer :uint8
                                                              (dtype/ecount gradient-line))
                                        [n-pixels 3])))
          gradient-line)
        (dtt/ensure-tensor))))


(defn colorize
  "Apply a color gradient to a tensor returning an image.  Takes A 1 or 2d tensor.
   If data-min, data-max aren't provided they are found in the data.
   A buffered image is returned.

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
  :gradient-default-n - When an IFn is provided, it is quantized over n steps."
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
          (let [valid-indexes (when check-invalid?
                                (dfn/argfilter dfn/valid?
                                               (dtype/->reader src-tens
                                                               :float64)))
               src-reader (if (and valid-indexes
                                        (not= (dtype/ecount valid-indexes)
                                              (dtype/ecount src-tens)))
                            (dtype/indexed-reader valid-indexes src-tens)
                            src-tens)]
            (merge
             (dfn/descriptive-stats src-reader [:min :max])
             {:src-reader src-reader
              :valid-indexes valid-indexes})))
        n-pixels (if valid-indexes
                   (dtype/ecount valid-indexes)
                   (dtype/ecount src-reader))
        data-min (double data-min)
        data-max (double data-max)
        _ (when (or (dfn/invalid? data-min)
                    (dfn/invalid? data-max))
            (throw (Exception. "NAN or INF in src data detected!")))
        data-range (- data-max data-min)
        src-reader (if-not (and (flp-close 0.0 data-min)
                                (flp-close 1.0 data-max))
                     (unary-op/unary-reader :float64
                                            (-> (- x data-min)
                                                (/ data-range))
                                            src-reader)
                     src-reader)
        src-reader (typecast/datatype->reader :float64 src-reader)
        img-type (if alpha?
                   :byte-abgr
                   :byte-bgr)
        res-image (case (count img-shape)
                    2 (bufimg/new-image (first img-shape) (second img-shape) img-type)
                    1 (bufimg/new-image 1 (first img-shape) img-type))
        ;;Flatten out src-tens and res-tens and make them readers
        n-channels (long (if alpha? 4 3))
        res-tens (dtt/reshape res-image [n-pixels n-channels])
        res-tens (tens-typecast/datatype->tensor-writer
                  :uint8 res-tens)
        gradient-line (gradient-name->gradient-line gradient-name invert-gradient?
                                                    gradient-default-n)
        n-gradient-increments (long (first (dtype/shape gradient-line)))
        gradient-line (tens-typecast/datatype->tensor-reader
                       :uint8
                       gradient-line)
        line-last-idx (double (dec n-gradient-increments))
        n-pixels (long (if valid-indexes
                         (dtype/ecount valid-indexes)
                         n-pixels))]
    (if alpha?
      (pfor/parallel-for
       idx
       n-pixels
       (let [src-val (.read src-reader idx)]
         (when (Double/isFinite src-val)
           (let [p-value (min 1.0 (max 0.0 src-val))
                 line-idx (long (Math/round (* p-value line-last-idx)))]
             ;;alpha channel first
             (.write2d res-tens idx 0 255)
             (.write2d res-tens idx 1 (.read2d gradient-line line-idx 0))
             (.write2d res-tens idx 2 (.read2d gradient-line line-idx 1))
             (.write2d res-tens idx 3 (.read2d gradient-line line-idx 2))))))
      (pfor/parallel-for
       idx
       n-pixels
       (let [src-val (.read src-reader idx)]
         (when (Double/isFinite src-val)
           (let [p-value (min 1.0 (max 0.0 (.read src-reader idx)))
                 line-idx (long (Math/round (* p-value line-last-idx)))]
             (.write2d res-tens idx 0 (.read2d gradient-line line-idx 0))
             (.write2d res-tens idx 1 (.read2d gradient-line line-idx 1))
             (.write2d res-tens idx 2 (.read2d gradient-line line-idx 2)))))))
    res-image))


(defn colorize->clj
  "Same as colorize but returns a ND sequence of [b g r] persistent vectors.
  For options, see documentation in colorize."
  [src-tens gradient-name & options]
  (when (seq src-tens)
    (let [src-dims (dtype/shape src-tens)]
      (-> (apply colorize src-tens gradient-name options)
          ;;In case of 1d.  colorize always returns buffered image which is always
          ;;2d.
          (dtt/reshape (concat src-dims [3]))
          (dtt/->jvm)))))


(defn- update-or-append-gradient
  [img-fname gradient-name]
  (let [png-img (bufimg/load img-fname)
        [height width n-chans] (dtype/shape png-img)
        ;;ensure known color palette and such
        png-img (bufimg/resize png-img 260 1 {:dst-img-type :byte-bgr})
        existing-map @gradient-map
        existing-tens @gradient-tens
        new-entry (get existing-map gradient-name
                       {:tensor-index (count existing-map)
                        :gradient-shape [260 3]})
        existing-map (assoc existing-map gradient-name new-entry)
        new-img (bufimg/new-image (count existing-map) 260 :byte-bgr)
        img-tens (dtt/ensure-tensor new-img)]
    (doseq [[grad-n {:keys [tensor-index gradient-shape]}] existing-map]
      (dtype/copy!
       (if (= gradient-name grad-n)
         (dtt/select png-img 0 :all :all)
         (dtt/select existing-tens tensor-index :all :all))
       (dtt/select img-tens tensor-index :all :all)))
    (spit "resources/gradients.edn" existing-map)
    (bufimg/save! new-img "resources/gradients.png")
    :ok))


(comment
  (require '[clojure.java.io :as io])
  (io/make-parents "gradient-demo/test.txt")

  (def test-src-tens (dtt/->tensor (repeat 128 (range 0 512))))
  (time (doseq [grad-name (keys @gradient-map)]
          (bufimg/save! (colorize test-src-tens grad-name)
                        "PNG"
                        (format "gradient-demo/%s.png" (name grad-name)))
          ))
  (defn bad-range
    [start end]
    (->> (range start end)
         (map (fn [item]
                (if (> (rand) 0.5)
                  Double/NaN
                  item)))))
  ;;Sometimes data has NAN's or INF's
  (def test-nan-tens (dtt/->tensor (repeatedly 128 #(bad-range 0 512))))

  (colorize test-nan-tens :temperature-map
                                             :alpha? true
                                             :check-invalid? true
                                             :invert-gradient? true
                                             :data-min 0
                                             :data-max 512)

  (bufimg/save! (colorize test-nan-tens :temperature-map
                                             :alpha? true
                                             :check-invalid? true
                                             :invert-gradient? true
                                             :data-min 0
                                             :data-max 512)
                                   "PNG"
                                   (format "gradient-demo/%s-nan.png"
                                           (name :temperature-map)))
  (dotimes [iter 100]
    (time (doseq [grad-name (keys @gradient-map)]
            (comment (bufimg/save! (colorize test-nan-tens grad-name
                                             :alpha? true
                                             :check-invalid? true
                                             :data-min 0
                                             :data-max 512)
                                   "PNG"
                                   (format "gradient-demo/%s-nan.png"
                                           (name grad-name))))
            (colorize test-nan-tens grad-name
                      :alpha? true
                      :check-invalid? true
                      :invert-gradient? true
                      :data-min 0
                      :data-max 512))))

  (def custom-gradient-tens (dtt/->tensor
                             (->> (range 100)
                                  (map (fn [idx]
                                         (let [p-value (/ (double idx)
                                                          (double 100))]
                                           [(* 255 p-value) 0 (* (- 1.0 p-value)
                                                                 255)]))))))

  (bufimg/save! (colorize test-src-tens custom-gradient-tens
                          :invert-gradient? true)
                "PNG"
                "gradient-demo/custom-tensor-gradient.png")

  (defn custom-gradient-fn
    [^double p-value]
    (let [one-m-p (- 1.0 p-value)]
      [(* 255 one-m-p) (* 255 p-value) (* 255 one-m-p)]))

  (bufimg/save! (colorize test-src-tens custom-gradient-fn
                          :invert-gradient? true)
                "PNG"
                "gradient-demo/custom-ifn-gradient.png")
  )
