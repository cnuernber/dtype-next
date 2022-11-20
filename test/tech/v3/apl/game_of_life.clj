(ns tech.v3.apl.game-of-life
  "https://youtu.be/a9xAKttWgP4"
  (:require [tech.v3.tensor :as tens]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dtype-fn]
            [tech.v3.datatype.bitmap :as bitmap]
            [clojure.test :refer :all]
            [ham-fisted.api :as hamf])
  (:import [java.util Set]))


(defn membership
  [lhs rhs]
  (let [membership-set (bitmap/->bitmap rhs)]
    (dtype/emap (hamf/long-predicate v (.contains membership-set v)) :boolean lhs)))


(defn apl-take
  "Negative numbers mean left-pad.  Positive numbers mean right-pad."
  [item new-shape]
  (let [item-shape (dtype/shape item)
        abs-new-shape (mapv #(Math/abs (int %)) new-shape)
        abs-min-shape (mapv min item-shape abs-new-shape)
        reshape-item (apply tens/select item (map range abs-min-shape))
        retval (tens/new-tensor abs-new-shape
                                :datatype (dtype/get-datatype item))
        copy-item (apply tens/select retval
                         (map (fn [n-elems orig-item]
                                (if (>= orig-item 0)
                                  (range n-elems)
                                  (take-last n-elems (range (- orig-item)))))
                              abs-min-shape
                              new-shape))]
    (dtype/copy! reshape-item copy-item)
    retval))


(defn rotate-vertical
  [tens amount]
  (let [n-shape (count (dtype/shape tens))
        offsets (->> (concat (repeat (- n-shape 1) 0)
                             [(- amount)])
                     vec)]
    (tens/rotate tens offsets)))


(defn rotate-horizontal
  [tens amount]
  (let [n-shape (count (dtype/shape tens))
        offsets (->> (concat [(- amount)]
                             (repeat (- n-shape 1) 0))
                     vec)]
    (tens/rotate tens offsets)))


(def range-tens (tens/->tensor (partition 3 (range 9)) :datatype :int8))

(def bool-tens (-> range-tens
                   (membership [1 2 3 4 7])
                   ;;convert to zeros/ones for display.
                   (tens/clone :datatype :int8)))

(def take-tens (apl-take bool-tens [5 7]))

(def right-rotate (rotate-vertical take-tens -2))

(def down-rotate (rotate-horizontal right-rotate -1))

(def R-matrix down-rotate)

(def rotate-arg [1 0 -1])

(def group-rotated (->> rotate-arg
                        (map (partial rotate-vertical down-rotate))))

(def table-rotated (->> rotate-arg
                        (mapcat (fn [rot-amount]
                                  (->> group-rotated
                                       (mapv #(rotate-horizontal
                                               % rot-amount)))))))


(def summed (apply dtype-fn/+ table-rotated))


(defn game-of-life-operator
  [original new-matrix]
  (-> (dtype/emap
       (fn ^long [^long x ^long y]
         (if (or (== 3 y)
                 (and (not (== 0 x)) (== 4 y)))
           1
           0))
       :int8
       original
       new-matrix)
      ;;Force the actual result to be calculated.  Else we would get a *huge* chain of
      ;;reader maps.  We don't have to specify the datatype here because the statement
      ;;above produced an int8 (byte) reader.
      (tens/clone :datatype :int8)))


(def next-gen (game-of-life-operator R-matrix summed))


(defn life
  [R]
  (->> (for [horz-amount rotate-arg
             vert-amount rotate-arg]
         (tens/rotate R [horz-amount vert-amount]))
       (apply dtype-fn/+)
       (game-of-life-operator R)))


(defn life-seq
  [R]
  (cons R (lazy-seq (life-seq (life R)))))


(def half-RR (apl-take R-matrix [-10 -20]))


(def RR (-> (apl-take R-matrix [-10 -20])
            (apl-take [15 35])))


(def end-state
  [[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
   [0 0 0 0 1 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 1 0]
   [0 0 0 0 1 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 1]
   [0 0 0 0 0 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 0 0 1]
   [0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 1 1 0]])


(deftest game-of-life-test
  (is (= end-state
         (->> (life-seq RR)
              (take 1000)
              last
              (tens/->jvm)))))


(defmacro ^:private center-coord
  [loc dshp]
  `(let [loc# (rem ~loc ~dshp)]
     (if (< loc# 0)
       (+ loc# ~dshp)
       loc#)))


(defn game-of-life-compute-op
  [input-tens]
  (let [[yshp xshp] (dtype/shape input-tens)
        yshp (long yshp)
        xshp (long xshp)
        input-tens (tens/ensure-tensor input-tens)]
    (-> (tens/compute-tensor
         [yshp xshp]
         (fn [^long y ^long x]
           (let [original (.ndReadLong input-tens y x)
                 conv-sum (long (loop [idx 0
                                       sum 0]
                                  (if (< idx 9)
                                    (let [conv-y (unchecked-dec (quot idx 3))
                                          conv-x (unchecked-dec (rem idx 3))
                                          y-coord (center-coord (+ y conv-y) yshp)
                                          x-coord (center-coord (+ x conv-x) xshp)]
                                      (recur (unchecked-inc idx)
                                             (+ sum (.ndReadLong input-tens y-coord
                                                                 x-coord))))
                                    sum)))]
             (if (or (== 3 conv-sum)
                     (and (not (== 0 original))
                          (== 4 conv-sum)))
               1
               0)))
         (dtype/elemwise-datatype input-tens))
        (dtype/clone))))

(defn life-compute-seq
  [input]
  (cons input (lazy-seq (life-compute-seq (game-of-life-compute-op input)))))


(deftest game-of-life-compute-test
  (is (= end-state
         (->> (life-compute-seq RR)
              (take 1000)
              last
              (tens/->jvm)))))
