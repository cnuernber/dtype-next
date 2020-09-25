(ns tech.v3.tensor.dimensions.global-to-local-test
  (:require [tech.v3.tensor.dimensions :as dims]
            [tech.v3.tensor.dimensions.global-to-local :as gtol]
            [tech.v3.tensor.dimensions.analytics :as dims-analytics]
            [tech.v3.datatype.functional :as dtype-fn]
            [clojure.test :refer [deftest is]]
            [clojure.pprint :as pp])
  (:import [tech.v3.datatype PrimitiveIO]))


(set! *unchecked-math* true)


(defn compare-reader-impls
  [base-dims expected-reduced-shape correct-addrs]
  (let [reduced-dims (dims-analytics/reduce-dimensionality base-dims)
        default-reader (gtol/elem-idx->addr-fn reduced-dims)
        ast-reader (gtol/get-or-create-reader reduced-dims)
        reduced-dims-ast (gtol/signature->ast
                          (gtol/reduced-dims->signature reduced-dims))]
    (is (= expected-reduced-shape
           (->> reduced-dims
                (map (fn [[k v]] [k (vec v)]))
                (into {}))))
    (is (dtype-fn/equals correct-addrs default-reader))
    (is (dtype-fn/equals correct-addrs ast-reader)
        (with-out-str (pp/pprint (:ast reduced-dims-ast))))))


(deftest strided-image-test
  (compare-reader-impls (dims/dimensions [2 4 4] [32 4 1])
                        {:shape [2 16]
                         :strides [32 1]
                         :offsets [0 0]
                         :shape-ecounts [2 16]
                         :shape-ecount-strides [16 1]}
                        [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 32 33 34
                         35 36 37 38 39 40 41 42 43 44 45 46 47]))


(deftest strided-image-reverse-rgb-test
    (compare-reader-impls (dims/dimensions [2 4 [3 2 1 0]] [32 4 1])
                          {:shape [2 4 [3 2 1 0]]
                           :strides [32 4 1]
                           :offsets [0 0 0]
                           :shape-ecounts [2 4 4]
                           :shape-ecount-strides [16 4 1]}
                          [3 2 1 0 7 6 5 4 11 10 9 8 15 14 13 12 35 34
                           33 32 39 38 37 36 43 42 41 40 47 46 45 44]))


(deftest strided-image-reverse-rgb--most-sig-dim-test
  (compare-reader-impls (dims/dimensions [[1 0] 4 4] [32 4 1])
                        {:shape [[1 0] 16]
                         :strides [32 1]
                         :offsets [0 0]
                         :shape-ecounts [2 16]
                         :shape-ecount-strides [16 1]}
                        [32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47
                         0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15]))

;;TODO - check broadcasting on leading dimension
(deftest leading-bcast-1
  (compare-reader-impls (-> (dims/dimensions [2 4 4]
                                             [32 4 1])
                            (dims/broadcast [4 4 4]))
                        {:shape [2 16]
                         :strides [32 1]
                         :offsets [0 0]
                         :shape-ecounts [4 16]
                         :shape-ecount-strides [16 1]}
                        [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 32 33 34
                         35 36 37 38 39 40 41 42 43 44 45 46 47 0 1 2 3
                         4 5 6 7 8 9 10 11 12 13 14 15 32 33 34 35 36 37
                         38 39 40 41 42 43 44 45 46 47]))

(deftest leading-bcast-2
  (compare-reader-impls (-> (dims/dimensions [2 4 4]
                                             [16 4 1])
                            (dims/broadcast [4 4 4]))
                        {:shape [32]
                         :strides [1]
                         :offsets [0]
                         :shape-ecounts [64]
                         :shape-ecount-strides [1]}
                        [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19
                         20 21 22 23 24 25 26 27 28 29 30 31 0 1 2 3 4 5 6
                         7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24
                         25 26 27 28 29 30 31]))


(deftest offsets
  (compare-reader-impls (-> (dims/dimensions [2 4 4] [32 4 1])
                            (dims/rotate [0 0 1])
                            (dims/broadcast [4 4 4]))
                        {:shape [2 4 4],
                         :strides [32 4 1],
                         :offsets [0 0 1],
                         :shape-ecounts [4 4 4],
                         :shape-ecount-strides [16 4 1]}
                        [1 2 3 0 5 6 7 4 9 10 11 8 13 14 15 12 33 34
                         35 32 37 38 39 36 41 42 43 40 45 46 47 44 1
                         2 3 0 5 6 7 4 9 10 11 8 13 14 15 12 33 34 35
                         32 37 38 39 36 41 42 43 40 45 46 47 44]))


(deftest offsets2
  (compare-reader-impls (-> (dims/dimensions [(int 4) 4] [4 1])
                            (dims/rotate [1 1])
                            (dims/broadcast [4 4]))
                        {:shape [4 4]
	                 :strides [4 1]
	                 :offsets [1 1]
	                 :shape-ecounts [4 4]
	                 :shape-ecount-strides [4 1]}
                        [5 6 7 4 9 10 11 8 13 14 15 12 1 2 3 0]))


(comment
  (do
    (println "Dimension indexing system reader timings")
    (let [base-dims (dims/dimensions [256 256 4] [8192 4 1])
          reduced-dims (dims-analytics/reduce-dimensionality base-dims)
          ^PrimitiveIO default-reader (gtol/elem-idx->addr-fn reduced-dims)
          ^PrimitiveIO ast-reader (gtol/get-or-create-reader reduced-dims)
          n-elems (.lsize default-reader)
          read-all-fn (fn [^PrimitiveIO rdr]
                        (dotimes [idx n-elems]
                          (.readLong rdr idx)))]
      (println "Default Reader:")
      (crit/quick-bench (read-all-fn default-reader))
      (println "AST Reader:")
      (crit/quick-bench (read-all-fn ast-reader))))

  (do
    (println "Dimension indirect indexing system reader timings")
    (let [base-dims (dims/dimensions [256 256 [3 2 1 0]] [8192 4 1])
          reduced-dims (dims-analytics/reduce-dimensionality base-dims)
          ^PrimitiveIO default-reader (gtol/elem-idx->addr-fn reduced-dims)
          ^PrimitiveIO ast-reader (gtol/get-or-create-reader reduced-dims)
          n-elems (.lsize default-reader)
          read-all-fn (fn [^PrimitiveIO rdr]
                        (dotimes [idx n-elems]
                          (.readLong rdr idx)))]
      (println "Default Reader:")
      (crit/quick-bench (read-all-fn default-reader))
      (println "AST Reader:")
      (crit/quick-bench (read-all-fn ast-reader))))


  )
