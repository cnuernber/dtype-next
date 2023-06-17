(ns tech.v3.tensor.pprint
  "Nicely print out a tensor of any size."
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.pprint :as dtype-pprint]
            [tech.v3.datatype.emap :as dtype-emap]
            ;;Base dtype implementation
            [tech.v3.datatype])
  (:import [java.lang StringBuilder]
           [tech.v3.datatype NDBuffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(def ^String NL (System/getProperty "line.separator"))

(defn- column-lengths
  "Finds the longest string length of each column in an array of Strings."
  [m]
  (let [item-shape (dtype-base/shape m)
        n-dims (count item-shape)
        ec (dtype-base/ecount m)]
    (if (and (> ec 0) (> n-dims 1))
      (let [n-cols (last item-shape)]
        (->> (range n-cols)
             (mapv (fn [col-idx]
                     (->> (dtype-proto/select m
                                              (concat (repeat (dec n-dims) :all)
                                                      [col-idx]))
                          dtype-base/->reader
                          (map #(.length ^String %))
                          (apply max 0))))))
      (mapv #(.length ^String %) (dtype-base/->reader m)))))


(defn- append-elem
  "Appends an element, right-padding up to a given column length."
  [^StringBuilder sb ^String elem ^long clen]
  (let [c (long (count elem))
        ws (- clen c)]
    (dotimes [_i ws]
      (.append sb \space))
    (.append sb elem)))


(defn- append-row
  "Appends a row of data."
  [^StringBuilder sb row column-lengths elipsis?]
  (let [column-lengths (dtype-base/->reader column-lengths)
        row (dtype-base/->reader row)
        cc (dtype-base/ecount column-lengths)]
    (.append sb \[)
    (dotimes [i cc]
      ;; the first element doesn't have a leading ws.
      (when (> i 0) (.append sb \space))
      (append-elem sb (.readObject row i) (.readLong column-lengths i))
      (when (and elipsis?
                 (= (inc i) (quot cc 2)))
        (.append sb " ...")))
    (.append sb \])))


(defn- rprint
  "Recursively joins each element with a leading line break and whitespace. If there are
  no elements left in the matrix it ends with a closing bracket."
  [^StringBuilder sb tens prefix column-lengths elipsis-vec]
  (let [tens-shape (dtype-base/shape tens)
        prefix (str prefix " ")
        n-dims (count tens-shape)
        elipsis? (first elipsis-vec)
        n-dim-items (first tens-shape)]
    (if (= 1 n-dims)
      (append-row sb tens column-lengths elipsis?)
      (do
        (.append sb \[)
        (dotimes [i n-dim-items]
          (when (> i 0)
            (.append sb NL)
            (.append sb prefix))
          (rprint sb (tens i)
                  prefix column-lengths (rest elipsis-vec))
          (when (and elipsis?
                     (= (inc i) (quot n-dim-items 2)))
            (.append sb "\n")
            (.append sb prefix)
            (.append sb "...")))
        (.append sb \])))))


(def *max-dim-count
  "Half of the maximum number of elements in a dimension.  If a dimension has more
  that this then there is an elipsis in the middle of it."
  3)


(def *max-elem-count
  "If we have above this number of total elements in the tensor then we start doing
  elipsis."
  1000)


(defn shape->elipsis-vec
  "Given a shape vector, return a vector of boolean's as to whether
  this dimension will be shortened and contain an elipsis in the middle."
  [shape]
  (let [ecount (apply * shape)]
    (if (<= ecount 1000)
      (->> (repeat (count shape) false)
           vec)
      (->> shape
           (map (fn [item-dim]
                  (> (long item-dim) (long (* 2 *max-dim-count)))))
           vec))))


(defn base-tensor->string
  "Pretty-prints a tensor. Returns a String containing the pretty-printed
  representation."
  ([tens]
    (base-tensor->string tens nil))
  ([tens {:keys [prefix formatter]}]
   (if (.allowsRead ^NDBuffer tens)
     (let [formatter (or formatter dtype-pprint/format-object)]
       (if (number? tens)
         (formatter tens)
         (let [
               ;;Construct the printable tensor.  The way numpy does this is here:
               ;;https://github.com/numpy/numpy/blob/master/numpy/core/arrayprint.py
               ;;We scan the shape to see if we are over an element-count threashold.
               ;;If we are, then we reshape the tensor keeping track of which
               ;;dimensions got reshaped and thus need an elipsis.
               item-shape (->> (dtype-base/shape tens)
                               (remove #{1})
                               (vec))
               ;;Account for shape of [1]
               item-shape (if (empty? item-shape)
                            [(dtype-base/ecount tens)]
                            item-shape)
               tens (dtype-base/reshape tens item-shape)
               elipsis-vec (shape->elipsis-vec item-shape)
               tens (->> (map (fn [dim elipsis?]
                                (if elipsis?
                                  (concat (range *max-dim-count)
                                          (range (- dim *max-dim-count)
                                                 dim))
                                  :all))
                              item-shape elipsis-vec)
                         (dtype-proto/select tens))
               ;;Format all entries in our reshaped tens.
               tens (->> (packing/unpack tens)
                         (dtype-emap/emap formatter :string))
               prefix (or prefix "")
               sb (StringBuilder.)
               column-lengths (column-lengths tens)]
           (rprint sb tens prefix column-lengths elipsis-vec)
           (.toString sb))))
     "{tensor data unreadable}")))


(defn tensor->string
  ^String [tens]
  (format "#tech.v3.tensor<%s>%s\n%s"
          (name (dtype-base/elemwise-datatype tens))
          (vec (dtype-base/shape tens))
          (base-tensor->string tens)))
