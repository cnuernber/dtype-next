(ns tech.v2.apl.game-of-life
  "https://youtu.be/a9xAKttWgP4"
  (:require [tech.v2.tensor :as tens]
            [tech.v2.datatype :as dtype]
            [tech.v2.datatype.functional :as dtype-fn]
            [tech.v2.datatype.boolean-op :as bool-op]
            [tech.v2.datatype.binary-op :as binary-op]
            [tech.v2.datatype.unary-op :as unary-op]
            [clojure.test :refer :all]))


(defn membership
  [lhs rhs]
  (let [membership-set (set (dtype/->vector rhs))]
    (bool-op/boolean-unary-reader
     :object
     (contains? membership-set x)
     lhs)))


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


(def range-tens (tens/reshape (vec (range 9)) [3 3]))

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
  ;;We do a typed reader here so that everything happens in byte space with no boxing.
  (-> (binary-op/binary-reader
       :int8
       ;;Clojure conservatively interprets all integers as longs so we have to specify
       ;;that we want a byte.
       (unchecked-byte
        (if (or (= 3 y)
                (and (not= 0 x)
                     (= 4 y)))
          1
          0))
       original
       new-matrix)
      ;;Force the actual result to be calculated.  Else we would get a *huge* chain of
      ;;reader maps.  We don't have to specify the datatype here because the statement
      ;;above produced an int8 (byte) reader.
      (tens/tensor-force)))


(def next-gen (game-of-life-operator R-matrix summed))


(defn life
  [R]
  (->> (for [horz-amount rotate-arg
             vert-amount rotate-arg]
         (tens/rotate R [horz-amount vert-amount]))
       ;;This doesn't help the dense version much but it gives
       ;;the sparse version at least some parallelism
       ;;Simple parallel reduction
       (partition-all 2)
       (pmap (fn [items]
               (if (= 2 (count items))
                 (apply dtype-fn/+ items)
                 (first items))))
       (apply dtype-fn/+)
       (game-of-life-operator R)))


(defn life-seq
  [R]
  (cons R (lazy-seq (life-seq (life R)))))


(def half-RR (apl-take R-matrix [-10 -20]))


(def RR (-> (apl-take R-matrix [-10 -20])
            (apl-take [15 35])))


(defn mat->pic-mat
  [R]
  (->> R
       (unary-op/unary-reader
        (if (= 0 (int x))
          (char 0x02DA)
          (char 0x2021)))))


(defn print-life-generations
  [& [n-gens]]
  (doseq [life-item (take (or n-gens 1000) (life-seq RR))]
    (println (mat->pic-mat life-item))
    (Thread/sleep 125)))


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
