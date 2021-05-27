(ns tech.v3.datatype.rolling
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1]]
            [tech.v3.datatype.casting :as casting]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype Buffer DoubleBuffer LongBuffer ObjectBuffer]
           [java.util Iterator]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- do-pad-last
  [n-pad item-seq advanced-item-seq]
  (let [n-pad (long n-pad)]
    (if (seq advanced-item-seq)
      (cons (first item-seq)
            (lazy-seq (do-pad-last n-pad (rest item-seq)
                                   (rest advanced-item-seq))))
      (when (>= n-pad 0)
        (cons (first item-seq)
              (lazy-seq (do-pad-last (dec n-pad) item-seq nil)))))))


(defn- pad-last
  [n-pad item-seq]
  (do-pad-last n-pad item-seq (rest item-seq)))


(defn- pad-sequence
  "Repeat the first and last members of the sequence pad times"
  [n-pad item-seq]
  (concat (repeat n-pad (first item-seq))
          (pad-last n-pad item-seq)))


(defn- fixed-window-sequence
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


(defn ^:no-doc windowed-reader
  (^Buffer [window-size window-fn item rel-position]
   (let [window-size (long window-size)
         n-pad (long (case rel-position
                       :center (quot (long window-size) 2)
                       :left (dec window-size)
                       :right 0))
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


(defn fixed-rolling-window
  "Return a lazily evaluated rolling window of window-fn applied to each window.  The
  iterable or sequence is padded such that there are the same number of values in the
  result as in the input with repeated elements padding the beginning and end of the original
  sequence.
  If input is an iterator, output is an lazy sequence.  If input is a reader,
  output is a reader.

  :Options

  * `:relative-window-position` - Defaults to `:center` - controls the window's relative positioning
  in the sequence.


  Example (all results are same length):

```clojure
user> (require '[tech.v3.datatype :as dtype])
nil
user> (require '[tech.v3.datatype.rolling :as rolling])
nil
user> (require '[tech.v3.datatype.functional :as dfn])
nil
  user> (rolling/fixed-rolling-window (range 20) 5 dfn/sum {:relative-window-position :left})
[0 1 3 6 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85]
user> (rolling/fixed-rolling-window (range 20) 5 dfn/sum {:relative-window-position :center})
[3 6 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 89 92]
user> (rolling/fixed-rolling-window (range 20) 5 dfn/sum {:relative-window-position :right})
[10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 89 92 94 95]
user>
```"
  ([item window-size window-fn options]
   (vectorized-dispatch-1
    (fn [_] (throw (ex-info "Rolling windows aren't defined on scalars" {})))
    (fn [_res-dtype item]
      (->> (pad-sequence (quot (long window-size) 2) item)
           (fixed-window-sequence window-size 1)
           (map window-fn)))
    (fn [_result-dtype item]
      (windowed-reader window-size window-fn item
                       (:relative-window-position options :center)))
    nil
    item))
  ([item window-size window-fn]
   (fixed-rolling-window item window-size window-fn nil)))


(deftype ObjectRollingIterator [^{:unsynchronized-mutable true
                                  :tag long} start-idx
                                ^{:unsynchronized-mutable true
                                  :tag long} end-idx
                                ^double window-length
                                ^double stepsize
                                ^long n-elems
                                ^long n-subset
                                tweener
                                ^Buffer data]
  Iterator
  (hasNext [this] (< start-idx n-subset))
  (next [this]
    (let [start-val (data start-idx)
          next-end-idx
          (long
           (loop [eidx end-idx]
             (if (or (>= eidx n-elems)
                     (>= (double (tweener start-val (data eidx))) window-length))
               eidx
               (recur (unchecked-inc eidx)))))
          retval (range start-idx next-end-idx)
          next-start-idx (unchecked-inc start-idx)
          next-start-idx
          (long (if (== 0.0 stepsize)
                  next-start-idx
                  (loop [eidx (unchecked-inc next-start-idx)]
                    (if (or (>= eidx n-elems)
                            (>= (double (tweener start-val (data eidx))) stepsize))
                      eidx
                      (recur (unchecked-inc eidx))))))]
      (set! start-idx next-start-idx)
      (set! end-idx next-end-idx)
      retval)))


(defn variable-rolling-window-indexes
    "Given a reader of monotonically increasing source data, a double window
  length and a comparison function that takes two elements of the src-data
  and returns a double return an iterable of ranges that describe the windows in
  index space.  Once a window is found start index will be incremented by
  stepsize amount as according to comp-fn or 1 if stepsize is not provided or
  0.0.

  * src-data - convertible to reader.
  * window-length - double window length amount in the space of comp-fn.


  Returns an iterable of clojure ranges.  Note the last N ranges will not be
  window-size in length; you can filter these out if you require exactly window-size
  windows.

  Options:

  * `:stepsize` - double stepsize amount in the space of comp-fn.
  * `:comp-fn` - defaults to (- rhs lhs), must return a double result that will be
     used to figure out the next window indexes and the amount to increment the
     start index in between windows.
  * `:n-subset` - defaults to n-elems, used when parallelizing rolling windows over
     a single buffer this will exit the iteration prematurely and must be less than
     src-data n-elems.

  Examples:

```clojure

tech.v3.datatype.rolling> (vec (variable-rolling-window-indexes
                                (range 20) 5))
[(0 1 2 3 4)
 (1 2 3 4 5)
 (2 3 4 5 6)
 (3 4 5 6 7)
 (4 5 6 7 8)
 (5 6 7 8 9)
 (6 7 8 9 10)
 (7 8 9 10 11)
 (8 9 10 11 12)
 (9 10 11 12 13)
 (10 11 12 13 14)
 (11 12 13 14 15)
 (12 13 14 15 16)
 (13 14 15 16 17)
 (14 15 16 17 18)
 (15 16 17 18 19)
 (16 17 18 19)
 (17 18 19)
 (18 19)
  (19)]

tech.v3.datatype.rolling> (vec (variable-rolling-window-indexes
                           (range 20) 5 {:stepsize 2}))
[(0 1 2 3 4)
 (2 3 4 5 6)
 (4 5 6 7 8)
 (6 7 8 9 10)
 (8 9 10 11 12)
 (10 11 12 13 14)
 (12 13 14 15 16)
 (14 15 16 17 18)
 (16 17 18 19)
 (18 19)]
```"
  (^Iterable [src-data window-length {:keys [stepsize
                                             comp-fn
                                             n-subset]
                                      :or {stepsize 0.0}}]
   (let [comp-fn (or comp-fn (fn [lhs rhs]
                               (double (- (double rhs)
                                          (double lhs)))))
         src-data (dtype-base/->buffer src-data)
         n-elems (.lsize src-data)
         n-subset (long (or n-subset n-elems))]
     (reify
       Iterable
       (iterator [this]
         (ObjectRollingIterator. 0 1
                                 (double window-length)
                                 (double stepsize)
                                 n-elems n-subset
                                 comp-fn src-data)))))
  (^Iterable [src-data window-length]
   (variable-rolling-window-indexes src-data window-length nil)))
