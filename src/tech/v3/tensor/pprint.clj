(ns tech.v3.tensor.pprint
  "Nicely print out a tensor of any size."
  (:require [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.unary-op :as unary]
            [tech.v3.datatype.pprint :as dtype-pprint])
  (:import [java.lang StringBuilder]
           [java.io Writer]
           [tech.v3.datatype PrimitiveNDIO]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)

(def ^String NL (System/getProperty "line.separator"))

(defn- column-lengths
  "Finds the longest string length of each column in an array of Strings."
  [m]
  (let [item-shape (dtype/shape m)
        n-dims (count item-shape)]
    (if (> n-dims 1)
      (let [n-cols (last item-shape)]
        (->> (range n-cols)
             (mapv (fn [col-idx]
                     (->> (.select ^PrimitiveNDIO m
                                   (concat (repeat (dec n-dims) :all)
                                           [col-idx]))
                          dtype/->reader
                          (map #(.length ^String %))
                          (apply max))))))
      (mapv #(.length ^String %) (dtype/->reader m)))))


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
        cc (dtype/ecount column-lengths)]
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
  [^StringBuilder sb ^PrimitiveNDIO tens prefix column-lengths elipsis-vec]
  (let [tens-shape (dtype/shape tens)
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
          (rprint sb (.ndReadObject tens i)
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
  ([^PrimitiveNDIO tens {:keys [prefix formatter]}]
   (if (.allowsRead tens)
     (let [formatter (or formatter dtype-pprint/format-object)]
       (if (number? tens)
         (formatter tens)
         (let [
               ;;Construct the printable tensor.  The way numpy does this is here:
               ;;https://github.com/numpy/numpy/blob/master/numpy/core/arrayprint.py
               ;;We scan the shape to see if we are over an element-count threashold.
               ;;If we are, then we reshape the tensor keeping track of which
               ;;dimensions got reshaped and thus need an elipsis.
               item-shape (.shape tens)
               elipsis-vec (shape->elipsis-vec item-shape)
               tens (->> (map (fn [dim elipsis?]
                                (if elipsis?
                                  (concat (range *max-dim-count)
                                          (range (- dim *max-dim-count)
                                                 dim))
                                  :all))
                              item-shape elipsis-vec)
                         (.select tens))
               ;;Format all entries in our reshaped tens.
               tens (->> (packing/unpack tens)
                         (unary/reader formatter :string))
               prefix (or prefix "")
               sb (StringBuilder.)
               column-lengths (column-lengths tens)]
           (rprint sb tens prefix column-lengths elipsis-vec)
           (.toString sb))))
     "{tensor data unreadable}")))


(defn tensor->string
  ^String [^PrimitiveNDIO tens]
  (format "#tech.v3.tensor<%s>%s\n%s"
          (name (dtype/get-datatype tens))
          (dtype/shape tens)
          (base-tensor->string tens)))
