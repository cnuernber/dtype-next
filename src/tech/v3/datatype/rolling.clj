(ns tech.v3.datatype.rolling
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1]]
            [tech.v3.datatype.casting :as casting]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype Buffer DoubleBuffer LongBuffer ObjectBuffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn do-pad-last
  [n-pad item-seq advanced-item-seq]
  (let [n-pad (long n-pad)]
    (if (seq advanced-item-seq)
      (cons (first item-seq)
            (lazy-seq (do-pad-last n-pad (rest item-seq)
                                   (rest advanced-item-seq))))
      (when (>= n-pad 0)
        (cons (first item-seq)
              (lazy-seq (do-pad-last (dec n-pad) item-seq nil)))))))


(defn pad-last
  [n-pad item-seq]
  (do-pad-last n-pad item-seq (rest item-seq)))


(defn pad-sequence
  "Repeat the first and last members of the sequence pad times"
  [n-pad item-seq]
  (concat (repeat n-pad (first item-seq))
          (pad-last n-pad item-seq)))


(defn fixed-window-sequence
  "Return a sequence of fixed windows.  Stops when the next window cannot
  be fulfilled."
  [window-size n-skip item-sequence]
  (let [window-size (long window-size)
        next-window (vec (take window-size item-sequence))]
    (when (= window-size (count next-window))
      (cons next-window (lazy-seq (fixed-window-sequence
                                   window-size n-skip
                                   (drop n-skip item-sequence)))))))


(defmacro ^:private window-idx
  [idx offset last-index]
  `(-> (pmath/+ ~idx ~offset)
       (pmath/max 0)
       (pmath/min ~last-index)))


(defn windowed-data-reader
  ^Buffer [window-size offset item]
  (let [item (dtype-base/->buffer item)
        item-dtype (dtype-base/elemwise-datatype item)
        window-size (long window-size)
        last-index (dec (.lsize item))
        offset (long offset)]
    (reify Buffer
      (elemwiseDatatype [rdr] item-dtype)
      (lsize [rdr] window-size)
      (readBoolean [this idx] (.readBoolean item (window-idx idx offset last-index)))
      (readByte [this idx] (.readByte item (window-idx idx offset last-index)))
      (readShort [this idx] (.readShort item (window-idx idx offset last-index)))
      (readChar [this idx] (.readChar item (window-idx idx offset last-index)))
      (readInt [this idx] (.readInt item (window-idx idx offset last-index)))
      (readLong [this idx] (.readLong item (window-idx idx offset last-index)))
      (readFloat [this idx] (.readFloat item (window-idx idx offset last-index)))
      (readDouble [this idx] (.readDouble item (window-idx idx offset last-index)))
      (readObject [this idx] (.readObject item (window-idx idx offset last-index)))
      (allowsRead [this] true)
      (allowsWrite [this] false))))


(defn windowed-reader
  (^Buffer [window-size window-fn item]
   (let [window-size (long window-size)
         n-pad (quot (long window-size) 2)
         elem-dtype (dtype-base/elemwise-datatype item)
         n-elems (dtype-base/ecount item)]
     (cond
       (casting/integer-type? elem-dtype)
       (reify LongBuffer
         (lsize [rdr] n-elems)
         (readLong [rdr idx]
           (unchecked-long
            (window-fn (windowed-data-reader window-size (- idx n-pad) item)))))
       (casting/float-type? elem-dtype)
       (reify DoubleBuffer
         (lsize [rdr] n-elems)
         (readDouble [rdr idx]
           (unchecked-double
            (window-fn (windowed-data-reader window-size (- idx n-pad) item)))))
       :else
       (reify ObjectBuffer
         (lsize [rdr] n-elems)
         (readObject [rdr idx]
           (window-fn (windowed-data-reader window-size (- idx n-pad) item))))))))


;;TODO test rolling windows
(defn fixed-rolling-window
  "Return a lazily evaluated rolling window of window-fn applied to each window.  The
  iterable or sequence is padded such that there are the same number of values in the
  result as in the input with repeated elements padding the beginning and end of the original
  sequence.
  If input is an iterator, output is an lazy sequence.  If input is a reader,
  output is a reader."
  [item window-size window-fn]
  (vectorized-dispatch-1 item
                         (fn [_] (throw (ex-info "Rolling windows aren't defined on scalars" {})))
                         (fn [item]
                           (->> (pad-sequence (quot (long window-size) 2) item)
                                (fixed-window-sequence window-size 1)
                                (map window-fn)))
                         (fn [item _result-dtype]
                           (windowed-reader window-size window-fn item))))
