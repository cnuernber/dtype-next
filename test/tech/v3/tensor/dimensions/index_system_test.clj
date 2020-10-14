(ns tech.v3.tensor.dimensions.index-system-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.tensor.dimensions.analytics :as dims-analytics]
            [tech.v3.tensor.dimensions.shape :as shape]
            [tech.v3.datatype.functional :as dtype-fn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.protocols :as dtype-proto]
            [clojure.pprint :as pp]
            [clojure.test :refer :all])
  (:import [tech.v3.datatype LongNDReader]))


(set! *warn-on-reflection* true)


(defn global-address->global-shape
  [global-address global-strides]
  (first
   (reduce (fn [[shape global-address] global-stride]
             (let [shape-idx (quot (int global-address)
                                   (int global-stride))]
               [(conj shape shape-idx)
                (rem (int global-address)
                     (int global-stride))]))
           [[] global-address]
           global-strides)))



(defn base-index-system-test
  ([shape strides offsets max-shape]
   (let [strides (dtype/->reader strides)
         max-stride-idx (argops/argmax strides)
         shape-dim-data (dtype/->reader (mapv (fn [shape-entry]
                                                (cond
                                                  (number? shape-entry)
                                                  (long shape-entry)
                                                  (dtype-proto/has-constant-time-min-max? shape-entry)
                                                  (inc (long (dtype-proto/constant-time-max shape-entry)))
                                                  :else
                                                  (+ 1 (apply max (dtype/->iterable shape-entry)))))
                                              shape))
         n-src-buffer-elems
         (* (.readLong strides max-stride-idx)
            (.readLong shape-dim-data max-stride-idx))
         n-elems (shape/ecount max-shape)
         dimensions (-> (dims/dimensions shape strides)
                        (dims/rotate offsets)
                        (dims/broadcast max-shape))
         max-strides (dims-analytics/shape-ary->strides max-shape)
         forward (dims/->global->local dimensions)
         backward (dims/->local->global dimensions)
         forward-full-elems
         (->> (range n-elems)
              (mapv (fn [global-address]
                      (let [local-address (.readLong forward (int global-address))
                            global-shape (global-address->global-shape
                                          global-address max-strides)]
                        [global-address
                         local-address
                         (.ndReadLongIter forward
                                          (dtype/->iterable global-shape))
                         (when (= 2 (count shape))
                           (.ndReadLong forward
                                        (first global-shape)
                                        (second global-shape)))
                         (when (= 3 (count shape))
                           (.ndReadLong forward
                                        (first global-shape)
                                        (second global-shape)
                                        (nth global-shape 2)))]))))

         forward-elems (mapv (comp vec (partial take 2)) forward-full-elems)
         backward-elems (->> (range n-src-buffer-elems)
                             (mapcat (fn [local-addr]
                                       (->> (.readObject backward (int local-addr))
                                            (map (fn [global-addr]
                                                   [global-addr local-addr])))))
                             (sort-by first))
         local-forward (mapv #(nth % 1) forward-full-elems)
         tens-read-forward (mapv #(nth % 2) forward-full-elems)
         tens-2d-read-forward (when (= 2 (count max-shape))
                                (mapv #(nth % 3) forward-full-elems))
         tens-3d-read-forward (when (= 3 (count max-shape))
                                (mapv #(nth % 4) forward-full-elems))
         explain-str (with-out-str
                       (pp/pprint {:shape shape
                                   :strides strides
                                   :offsets (vec offsets)
                                   :max-shape max-shape}))]

     ;;Make sure all addressing systems agree
     (is (= local-forward
            tens-read-forward)
         (str "read-forward" explain-str))

     (when (= 2 (count max-shape))
       (is (= local-forward
              tens-2d-read-forward)
           (str "2d-read" explain-str)))

     (when (= 3 (count max-shape))
       (is (= local-forward
              tens-3d-read-forward)
           (str "3d-read" explain-str)))

     (is (= (vec forward-elems)
            (vec backward-elems))
         (str "forward, backward" explain-str))))
  ([shape strides max-shape]
   (base-index-system-test shape strides
                           (dtype/const-reader 0 (dtype/ecount shape))
                           max-shape)))


(deftest test-the-systems
  (base-index-system-test [2 2] [2 1] [2 2])
  ;;rotate down
  (base-index-system-test [2 2] [2 1] [0 1] [2 2])
  (base-index-system-test [3 3] [3 1] [0 1] [3 3])
  (base-index-system-test [3 3] [3 1] [2 0] [3 3])

  (base-index-system-test [3 3] [3 1] [0 1] [3 3])
  (base-index-system-test [2 2] [2 1] [2 4])
  (base-index-system-test [2 2] [2 1] [2 6])
  (base-index-system-test [2 2] [2 1] [4 4])
  (base-index-system-test [2 2] [1 2] [2 2])
  (base-index-system-test [2 2] [1 2] [2 4])
  (base-index-system-test [2 2] [1 2] [4 2])



  (base-index-system-test [2 [1 0]] [2 1] [4 2])
  (base-index-system-test [2 [1 0]] [1 2] [4 6])

  ;;Check offseting *and* indexing
  (base-index-system-test [2 [1 0]] [2 1] [0 1] [4 2])
  (base-index-system-test [2 [1 0]] [1 2] [0 1] [4 6])

  (base-index-system-test [2 2] [2 4] [2 2])
  (base-index-system-test [2 2] [2 4] [2 4])
  (base-index-system-test [2 2] [2 4] [4 2])

  (base-index-system-test [2 2] [4 2] [2 2])
  (base-index-system-test [2 2] [4 2] [2 4])
  (base-index-system-test [2 2] [4 2] [4 6])

  (base-index-system-test [2 [1 0]] [4 2] [2 2])


  (base-index-system-test [2 2] [3 1] [2 2])

  (base-index-system-test [2 2] [3 1] [2 4])
  (base-index-system-test [2 2] [1 3] [4 4])

  ;;Normal image
  (base-index-system-test [4 4 3] [12 3 1] [4 4 3])

  ;;Image in channels-first
  (base-index-system-test [3 4 4] [1 12 3] [3 4 4])
  ;;Channels first and broadcast
  (base-index-system-test [3 4 4] [1 12 3] [12 4 4])

  (base-index-system-test [3 [1 2]] [1 3] [3 2])
  )
