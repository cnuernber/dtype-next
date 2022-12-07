(ns tech.v3.tensor.integration-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.tensor :as dtt]
            [ham-fisted.api :as hamf]
            [com.github.ztellman.primitive-math :as pmath]
            [clojure.test :refer [deftest is]])
  (:import [tech.v3.datatype NDBuffer]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(deftest tensor->array-and-back
  (let [test-tens (dtt/->tensor (partition 3 (range 9)))]
    (doseq [row (dtt/rows test-tens)]
      (is (= (dtt/->jvm row)
             (vec (dtype/make-container :java-array :int64 row)))))

    (doseq [col (dtt/columns test-tens)]
      (is (= (dtt/->jvm col)
             (vec (dtype/make-container :java-array :int64 col)))))))


(deftest tensor->list-and-back
  (let [test-tens (dtt/->tensor (partition 3 (range 9)))]
    (doseq [row (dtt/rows test-tens)]
      (is (= (dtt/->jvm row)
             (vec (dtype/make-container :list :int64 row)))))

    (doseq [col (dtt/columns test-tens)]
      (is (= (dtt/->jvm col)
             (vec (dtype/make-container :list :int64 col)))))))


(deftest block-rows->tensor
  (let [block-rows (repeatedly 4 #(int-array (range 5)))
        tensor (dtt/new-tensor (dtype/shape block-rows)
                               :datatype :int32)]
    (dtype/copy-raw->item! block-rows tensor)
    (is (= [[0 1 2 3 4]
            [0 1 2 3 4]
            [0 1 2 3 4]
            [0 1 2 3 4]]
           (dtt/->jvm tensor)))))


(deftest modify-time-test
  (let [source-image (dtt/new-tensor [512 288 3] :datatype :uint8)
        ;; Reader composition is lazy so the expression below reads from
        ;; the test image (ecount image) times.  It writes to the destination
        ;; once and the byte value is completely transformed from the src image
        ;; to the dest while in cache.  Virtual table lookups happen multiple
        ;; times per byte value.  ;; It is important to realize that under the
        ;; covers the image is stored as bytes.  These are read in a datatype-aware
        ;; way and converted to their appropriate unsigned values automatically
        ;; and when writter they are checked to ensure they are within range.
        ;; There are 2N checks for correct datatype in this pathway; everything else
        ;; is read/operated on as a short integer.
        reader-composition  #(-> source-image
                                 (dtt/select :all :all (long-array [2 1 0]))
                                 (dfn/+ 50)
                                 ;;Clamp top end to 0-255
                                 (dfn/min 255)
                                 (dtype/copy! (dtt/new-tensor [512 288 3]
                                                              :datatype :uint8)))

        inline-fn #(as-> source-image dest-image
                     (dtt/select dest-image :all :all [2 1 0])
                     (dtype/emap (hamf/long-unary-operator
                                  x
                                  (-> (+ x 50)
                                      (min 255)))
                                 :int16
                                 dest-image)
                     (dtype/copy! dest-image
                                  ;;Note from-prototype fails for reader chains.
                                  ;;So you have to copy or use an actual image.
                                  (dtt/new-tensor [512 288 3] :datatype :uint8)))
        compute-tensor #(-> (dtt/typed-compute-tensor
                             :int64 :uint8 3 (dtype/shape source-image)
                             [y x c]
                             (let [c (- 2 c)
                                   src-val (.ndReadLong source-image y x c)]
                               (-> src-val
                                   (pmath/+ 50)
                                   (pmath/min 255))))
                            (dtype/copy! (dtt/new-tensor [512 288 3] :datatype :uint8)))]
    ;;warm up and actually check that tostring works as expected
    (is (string? (.toString ^Object (reader-composition))))
    (is (string? (.toString ^Object (inline-fn))))
    (clojure.pprint/pprint
     {:reader-composition (with-out-str (time (dotimes [iter 10]
                                                (reader-composition))))
      :inline-fn (with-out-str (time (dotimes [iter 10]
                                       (inline-fn))))
      :compute-tensor (with-out-str (time (dotimes [iter 10]
                                            (compute-tensor))))})))


(deftest nd-buffer-descriptor
  ;; Test that we can get buffer descriptors from tensors.  We should also be able
  ;; to get buffer descriptors from nio buffers if they are direct mapped.
  (let [test-tensor (dtt/->tensor (->> (range 9)
                                       (partition 3))
                                  :datatype :float64)]
    (is (not (dtype/as-nd-buffer-descriptor test-tensor)))
    (is (-> (dtt/ensure-nd-buffer-descriptor test-tensor)
            :ptr))
    (let [new-tens (dtt/nd-buffer-descriptor->tensor
                    (dtt/ensure-nd-buffer-descriptor test-tensor))]
      (is (dfn/equals test-tensor new-tens))
      (let [trans-tens (dtt/transpose new-tens [1 0])
            trans-desc (dtype/as-nd-buffer-descriptor trans-tens)]
        (is (= {:datatype :tensor,
                :elemwise-datatype :float64,
                :endianness :little-endian,
                :shape [3 3],
                :strides [8 24]}
               (dissoc trans-desc :ptr :native-buffer)))))))


(deftest tensor-writers
  []
  (let [test-dim 10
        test-tens (dtt/new-tensor [test-dim test-dim 4] :datatype :uint8)
        reshape-tens (dtt/reshape test-tens [(* test-dim test-dim) 4])
        test-indexes [35 69 83 58 57 13 64 68 48 55 20 33 2 36
                      21 17 88 94 91 85]
        idx-tens (dtt/select reshape-tens (long-array test-indexes) :all)
        ^NDBuffer writer idx-tens]
    (dotimes [iter (count test-indexes)]
      (.ndWriteLong writer iter 3 255))
    (is (dfn/equals (sort test-indexes)
                    (argops/argfilter #(not= 0 %)
                                      (dtt/select test-tens :all :all 3))))))


(deftest normal-tensor-select
  (let [test-tens (-> (dtt/->tensor (partition 3 (range 9)))
                      (dtt/select (concat [0 1] [0 2]) :all))]
    (is (dfn/equals (dtt/->tensor [[0.000 1.000 2.000]
                                    [3.000 4.000 5.000]
                                    [0.000 1.000 2.000]
                                    [6.000 7.000 8.000]])
                    test-tens))))


(deftest tensor-destructure
  (let [[a b c] (first (dtt/->tensor (partition 3 (range 9))
                                      :datatype :int64))]
    (is (= [a b c]
           [0 1 2]))))


(deftest simple-clone
  (let [src-tens (dtt/reshape (range (* 1 128 256)) [1 128 256])
        sel-tens (dtt/select src-tens 0 (range 3) (range 3))]
    (is (= [[0 1 2] [256 257 258] [512 513 514]]
           (-> sel-tens
               ;; set the type to something we can test against
               (dtt/clone :datatype :int32)
               (dtt/->jvm))))))


(deftest ensure-tensor
  (is (dfn/equals (dtt/ensure-tensor [[0.0 0.2 0.4 0.6 0.8 1.0]
                                      [0.0 0.2 0.4 0.6 0.8 1.0]])
                  (dtt/->tensor [[0.0 0.2 0.4 0.6 0.8 1.0]
                                 [0.0 0.2 0.4 0.6 0.8 1.0]]))))


(deftest select-failure
  (let [src-tens (dtt/->tensor (partition 3 (range 60)))
        selected-rows (->> (dtt/select src-tens [12 16 19] :all)
                           (dtt/rows)
                           (mapv vec))]
    (is (= [[36 37 38] [48 49 50] [57 58 59]]
           selected-rows))))


(deftest reduce-axis
  (let [src-tens (dtt/->tensor (partition 3 (range 12)))]
    (is (= [18.0 22.0 26.0]
           (vec (dtt/reduce-axis dfn/sum src-tens 0))))
    (is (= [3.0 12.0 21.0 30.0]
           (vec (dtt/reduce-axis dfn/sum src-tens 1)))))
  (let [src-tens (dtt/->tensor (partition 3 (partition 5 (range 30))))]
    (is (= [2 3 5]
           (dtype/shape src-tens)))
    (is (dfn/equals (dtt/->tensor [[15.00 17.00 19.00 21.00 23.00]
                                   [25.00 27.00 29.00 31.00 33.00]
                                   [35.00 37.00 39.00 41.00 43.00]])
                    (dtt/reduce-axis dfn/sum src-tens 0)))
    (is (dfn/equals (dtt/->tensor [[15.00 18.00 21.00 24.00 27.00]
                                   [60.00 63.00 66.00 69.00 72.00]])
                    (dtt/reduce-axis dfn/sum src-tens 1)))
    (is (dfn/equals (dtt/->tensor [[10.00 35.00 60.00]
                                   [85.00 110.0 135.0]])
                    (dtt/reduce-axis dfn/sum src-tens 2)))))


(defn array-of-array->tensor
  []
  (let [d2 (->> (range 2000000)
                (partition 2)
                (map double-array)
                into-array)]
    (println "nested array of tensor shape" (dtype/shape d2))
    (time (dtt/->tensor d2)))
  (let [d4 (->> (range 2000000)
                (partition 4)
                (map double-array)
                into-array)]
    (println "nested array of tensor shape" (dtype/shape d4))
    (time (dtt/->tensor d4)))
  (let [d100 (->> (range 2000000)
                (partition 100)
                (map double-array)
                into-array)]
    (println "nested array of tensor shape" (dtype/shape d100))
    (time (dtt/->tensor d100)))
  (let [d10000 (->> (range 2000000)
                   (partition 10000)
                   (map double-array)
                   into-array)]
    (println "nested array of tensor shape" (dtype/shape d10000))
    (time (dtt/->tensor d10000))))


(deftest set-value-mset-constant
  (let [test-tens (dtt/->tensor (->> (range 27)
                                     (partition 3)
                                     (partition 3))
                                :datatype :float64)
        sv-tens (dtt/reshape (double-array [1 2 3]) [3 1])
        tt1 (dtype/set-value! (dtype/clone test-tens) [:all :all (range 2)] 0)
        ;;broadcast test
        tt2 (dtype/set-value! (dtype/clone test-tens) [:all :all (range 2)] sv-tens)]

    (is (= (dtt/->jvm tt1)
           [[[0.000 0.000 2.000]
             [0.000 0.000 5.000]
             [0.000 0.000 8.000]]
            [[0.000 0.000 11.00]
             [0.000 0.000 14.00]
             [0.000 0.000 17.00]]
            [[0.000 0.000 20.00]
             [0.000 0.000 23.00]
             [0.000 0.000 26.00]]]))
    (is (= (dtt/->jvm tt2)
           [[[1.000 1.000 2.000]
             [2.000 2.000 5.000]
             [3.000 3.000 8.000]]
            [[1.000 1.000 11.00]
             [2.000 2.000 14.00]
             [3.000 3.000 17.00]]
            [[1.000 1.000 20.00]
             [2.000 2.000 23.00]
             [3.000 3.000 26.00]]]))))


(deftest broadcast-left-extend-shape
  (let [target-shape [2 3 4]
        src-data (dtt/ensure-tensor [1 2 3 4])
        target (dtt/broadcast src-data target-shape)]
    (is (= target-shape (dtype/shape target)))))


(deftest mset-auto-broadcast
  (let [src-tens (dtt/new-tensor [4 4 4] :datatype :uint8)
        ;;Logically this should auto-broadcast
        src-tens (dtt/mset! src-tens [0x20 0x20 0x20 0xFF])]
    (is (= [0x20 0x20 0x20 0xFF]
           (vec (dtt/mget src-tens 0 0))))))
