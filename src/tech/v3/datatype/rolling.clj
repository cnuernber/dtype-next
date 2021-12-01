(ns tech.v3.datatype.rolling
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dt-proto]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1]]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.emap :as emap])
  (:import [tech.v3.datatype Buffer LongReader DoubleReader ObjectReader]
           [java.util Iterator]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)



(deftype WindowRange [^long start-idx ^long n-elems]
  LongReader
  (lsize [_this] n-elems)
  (readLong [_this idx]
    (+ start-idx idx)))


(defmacro ^:private padded-read
  [src-data idx win-start n-src left-pad-val right-pad-val read-fn]
  `(let [idx# (+ ~idx ~win-start)]
     (cond
       (< idx# 0) ~left-pad-val
       (>= idx# ~n-src) ~right-pad-val
       :else (~read-fn ~src-data idx#))))


(defn window-ranges->window-reader
  "Given src data, reader of window ranges and an edge mode return
  a reader that when read returns a reader of src-data appropriately padded
  depending on edge mode.

  * src-data - buffer of source data
  * win-ranges - reader of WindowRange.
  * edge-mode - one of `:zero` pad with 0/nil or `:clamp` in which case the padding
    is the first/last values in src data, respectively.

  Example:
```clojure
tech.v3.datatype.rolling> (window-ranges->window-reader
                           (range 10) (fixed-rolling-window-ranges 10 3 :center)
                           :clamp)
[[0 0 1] [0 1 2] [1 2 3] [2 3 4] [3 4 5] [4 5 6] [5 6 7] [6 7 8] [7 8 9] [8 9 9]]
tech.v3.datatype.rolling> (window-ranges->window-reader
                           (range 10) (fixed-rolling-window-ranges 10 3 :center)
                           :zero)
[[0 0 1] [0 1 2] [1 2 3] [2 3 4] [3 4 5] [4 5 6] [5 6 7] [6 7 8] [7 8 9] [8 9 0]]
```"
  [src-data win-ranges edge-mode]
  (let [op-type (dt-proto/operational-elemwise-datatype src-data)
        src-data (dtype-base/->reader src-data op-type)
        win-ranges (dtype-base/->reader win-ranges)
        n-src (.lsize src-data)
        n-win (.lsize win-ranges)
        src-dt (dtype-base/elemwise-datatype src-data)
        [left-pad-val right-pad-val]
        (if (casting/numeric-type? (packing/unpack-datatype src-dt))
          (case edge-mode
            :zero [(casting/cast 0 src-dt) (casting/cast 0 src-dt)]
            :clamp [(src-data 0) (src-data (dec n-src))])
          (case edge-mode
            :zero [nil nil]
            :clamp [(src-data 0) (src-data (dec n-src))]))
        src-unpack-dtype (packing/unpack-datatype src-dt)]
    (reify ObjectReader
      (lsize [this] n-win)
      (readObject [this idx]
        (let [^WindowRange range (.readObject win-ranges idx)
              win-start (.start-idx range)
              win-n-elems (.n-elems range)
              win-end (+ win-start (.n-elems range))]
          ;;Sub-buffer will always be faster than anything else and allows
          ;;conversion of src-data back into array/native buffer
          (if (and (>= win-start 0)
                   (<= win-end n-src))
            (dtype-base/sub-buffer src-data win-start win-n-elems)
            (case (casting/simple-operation-space src-unpack-dtype)
              :int64 (reify LongReader
                       (elemwiseDatatype [rdr] src-unpack-dtype)
                       (lsize [rdr] win-n-elems)
                       (readLong [rdr idx]
                         (unchecked-long
                          (padded-read src-data idx win-start n-src
                                       left-pad-val right-pad-val .readLong))))
              :float64 (reify DoubleReader
                         (elemwiseDatatype [rdr] src-unpack-dtype)
                         (lsize [rdr] win-n-elems)
                         (readDouble [rdr idx]
                           (if-let [retval
                                    (padded-read src-data idx win-start n-src
                                                 left-pad-val right-pad-val .readDouble)]
                             (unchecked-double retval)
                             Double/NaN)))
              (reify ObjectReader
                (elemwiseDatatype [rdr] src-unpack-dtype)
                (lsize [rdr] win-n-elems)
                (readObject [rdr idx]
                  (padded-read src-data idx win-start n-src
                               left-pad-val right-pad-val .readObject))))))))))


(defn fixed-rolling-window-ranges
  "Return a reader of window-ranges of n-elems length.

  Example:

```clojure
tech.v3.datatype.rolling> (fixed-rolling-window-ranges 10 3 :left)
[[-2 -1 0] [-1 0 1] [0 1 2] [1 2 3] [2 3 4] [3 4 5] [4 5 6] [5 6 7] [6 7 8] [7 8 9]]
tech.v3.datatype.rolling> (fixed-rolling-window-ranges 10 3 :center)
[[-1 0 1] [0 1 2] [1 2 3] [2 3 4] [3 4 5] [4 5 6] [5 6 7] [6 7 8] [7 8 9] [8 9 10]]
tech.v3.datatype.rolling> (fixed-rolling-window-ranges 10 3 :right)
[[0 1 2] [1 2 3] [2 3 4] [3 4 5] [4 5 6] [5 6 7] [6 7 8] [7 8 9] [8 9 10] [9 10 11]]
```"
  [n-elems window-size relative-window-position]
  (let [n-elems (long n-elems)
        window-size (long window-size)
        padding (long (case relative-window-position
                        :center (- (quot (long window-size) 2))
                        :left (- (dec window-size))
                        :right 0))]
    (reify ObjectReader
      (lsize [this] n-elems)
      (readObject [this idx]
        (WindowRange. (+ idx padding) window-size)))))


(defn- fixed-reader-rolling-window
  [item window-size relative-window-position edge-mode window-fn options]
  (let [n-elems (dtype-base/ecount item)]
    (->> (window-ranges->window-reader
          item (fixed-rolling-window-ranges n-elems window-size
                                            relative-window-position)
          edge-mode)
         (emap/emap window-fn
                    (:datatype options
                               (packing/unpack-datatype
                                (dtype-base/elemwise-datatype item)))))))


(defn fixed-rolling-window
  "Return a lazily evaluated rolling window of window-fn applied to each window.  The
  iterable or sequence is padded such that there are the same number of values in the
  result as in the input with repeated elements padding the beginning and end of the original
  sequence.
  If input is an iterator, output is an lazy sequence.  If input is a reader,
  output is a reader.

  :Options

  * `:relative-window-position` - Defaults to `:center` - controls the window's
  relative positioning in the sequence.
  * `:edge-mode` - Defaults to `:clamp` - either `:zero` in which case window values
  off the edge are zero for numeric types or nil for object types or `:clamp` - in
  which case window values off the edge of the data are bound to the first or last
  values respectively.


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
  ([item window-size window-fn {:keys [relative-window-position
                                       edge-mode
                                       _datatype]
                                :or {relative-window-position :center
                                     edge-mode :clamp}
                                :as options}]
   (vectorized-dispatch-1
    (fn [_] (throw (ex-info "Rolling windows aren't defined on scalars" {})))
    (fn [_res-dtype item]
      (fixed-reader-rolling-window (vec item) window-size relative-window-position
                                   edge-mode window-fn options))
    (fn [_result-dtype item]
      (fixed-reader-rolling-window item window-size relative-window-position
                                   edge-mode window-fn options))
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
  (hasNext [_this] (< start-idx n-subset))
  (next [_this]
    (let [start-val (data start-idx)
          next-end-idx
          (long
           (loop [eidx end-idx]
             (if (or (>= eidx n-elems)
                     (>= (double (tweener start-val (data eidx))) window-length))
               eidx
               (recur (unchecked-inc eidx)))))
          retval (WindowRange. start-idx (- next-end-idx start-idx))
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


(defn variable-rolling-window-ranges
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
tech.v3.datatype.rolling> (vec (variable-rolling-window-ranges
                                (range 20) 5))
[[0 1 2 3 4]
 [1 2 3 4 5]
 [2 3 4 5 6]
 [3 4 5 6 7]
 [4 5 6 7 8]
 [5 6 7 8 9]
 [6 7 8 9 10]
 [7 8 9 10 11]
 [8 9 10 11 12]
 [9 10 11 12 13]
 [10 11 12 13 14]
 [11 12 13 14 15]
 [12 13 14 15 16]
 [13 14 15 16 17]
 [14 15 16 17 18]
 [15 16 17 18 19]
 [16 17 18 19]
 [17 18 19]
 [18 19]
 [19]]

tech.v3.datatype.rolling> (vec (variable-rolling-window-ranges
                           (range 20) 5 {:stepsize 2}))
[[0 1 2 3 4]
 [2 3 4 5 6]
 [4 5 6 7 8]
 [6 7 8 9 10]
 [8 9 10 11 12]
 [10 11 12 13 14]
 [12 13 14 15 16]
 [14 15 16 17 18]
 [16 17 18 19]
 [18 19]]
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
   (variable-rolling-window-ranges src-data window-length nil)))


(defn expanding-window-ranges
  "Return a reader of expanding window ranges used for cumsum type operations."
  [n-elems]
  (let [n-elems (long n-elems)]
    (reify ObjectReader
      (lsize [this] n-elems)
      (readObject [this idx]
        (WindowRange. 0 (inc idx))))))
