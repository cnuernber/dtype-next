(ns tech.v3.tensor.integration-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.tensor :as dtt]
            [clojure.test :refer :all]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(deftest tensor->array-and-back
  (let [test-tens (dtt/->tensor (partition 3 (range 9)))]
    (doseq [row (dtt/rows test-tens)]
      (is (= (dtt/->jvm row)
             (vec (dtype/make-container :java-array :float32 row)))))

    (doseq [col (dtt/columns test-tens)]
      (is (= (dtt/->jvm col)
             (vec (dtype/make-container :java-array :float32 col)))))))


(deftest tensor->list-and-back
  (let [test-tens (dtt/->tensor (partition 3 (range 9)))]
    (doseq [row (dtt/rows test-tens)]
      (is (= (dtt/->jvm row)
             (vec (dtype/make-container :list :float32 row)))))

    (doseq [col (dtt/columns test-tens)]
      (is (= (dtt/->jvm col)
             (vec (dtype/make-container :list :float32 col)))))))


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
                                 (dtype/select :all :all [2 1 0])
                                 (dfn/+ 50)
                                 ;;Clamp top end to 0-255
                                 (dfn/min 255)
                                 (dtype/copy! (dtype/clone source-image)))

        inline-fn #(as-> source-image dest-image
                     (dtype/select dest-image :all :all [2 1 0])
                     (dtype/emap (fn [^long x]
                                   (-> (+ x 50)
                                       (min 255)
                                       unchecked-short))
                                 :int16
                                 dest-image)
                     (dtype/copy! dest-image
                                  ;;Note from-prototype fails for reader chains.
                                  ;;So you have to copy or use an actual image.
                                  (dtype/clone source-image)))]
    ;;warm up and actually check that tostring works as expected
    (is (string? (.toString ^Object (reader-composition))))
    (is (string? (.toString ^Object (inline-fn))))
    (clojure.pprint/pprint
     {:reader-composition (with-out-str (time (dotimes [iter 10]
                                                (reader-composition))))
      :inline-fn (with-out-str (time (dotimes [iter 10]
                                       (inline-fn))))})))


(deftest buffer-descriptor
  ;; Test that we can get buffer descriptors from tensors.  We should also be able
  ;; to get buffer descriptors from nio buffers if they are direct mapped.
  (let [test-tensor (dtt/->tensor (->> (range 9)
                                        (partition 3)))]
    (is (not (dtype/as-buffer-descriptor test-tensor)))
    (is (-> (dtt/ensure-buffer-descriptor test-tensor)
            :ptr))
    (let [new-tens (dtt/buffer-descriptor->tensor
                    (dtt/ensure-buffer-descriptor test-tensor))]
      (is (dfn/equals test-tensor new-tens))
      (let [trans-tens (dtt/transpose new-tens [1 0])
            trans-desc (dtype/as-buffer-descriptor trans-tens)]
        (is (= {:datatype :float64, :shape [3 3], :strides [8 24]}
               (dissoc trans-desc :ptr)))))))


(deftest tensor-writers
  []
  (let [test-dim 10
        test-tens (dtt/new-tensor [test-dim test-dim 4] :datatype :uint8)
        reshape-tens (dtt/reshape test-tens [(* test-dim test-dim) 4])
        test-indexes [35 69 83 58 57 13 64 68 48 55 20 33 2 36
                      21 17 88 94 91 85]
        idx-tens (dtype/select reshape-tens (long-array test-indexes) :all)
        ^PrimitiveNDIO writer idx-tens]
    (dotimes [iter (count test-indexes)]
      (.ndWriteLong writer iter 3 255))
    (is (dfn/equals (sort test-indexes)
                    (dfn/argfilter #(not= 0 %)
                                   (dtype/select test-tens :all :all 3))))))


(deftest normal-tensor-select
  (let [test-tens (-> (dtt/->tensor (partition 3 (range 9)))
                      (dtype/select (concat [0 1] [0 2]) :all))]
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
        sel-tens (dtype/select src-tens 0 (range 3) (range 3))]
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


(defn strided-tensor-copy-time-test
  []
  (let [src-tens (-> (dtt/new-tensor [2048 2048 4] :datatype :uint8)
                     (dtype/select (range 256 (* 2 256))
                                  (range 256 (* 2 256))
                                  :all))
        dst-tens (dtt/new-tensor [256 256 4] :datatype :uint8)]
    ;; (dtype/copy! src-tens dst-tens)
    (dtype/copy! src-tens dst-tens)))


(defn strided-tensor-bit-blit-test
  []
  (let [src-tens (-> (dtt/new-tensor [2048 2048 4] :datatype :uint8)
                     (dtype/select (range 256 (* 2 256))
                                 (range 256 (* 2 256))
                                 :all))
        dst-tens (dtt/new-tensor [256 256 4] :datatype :uint8)]
    ;; (dtype/copy! src-tens dst-tens)
    (assert (= :ok (tc/bit-blit! src-tens dst-tens {})))))


(defn read-time-test
  []
  (let [src-tens (dtt/new-tensor [2048 2048 4] :datatype :uint8)]
    (let [src-tens (dtype/select src-tens (range 256 (* 2 256))
                               (range 256 (* 2 256))
                               :all)
          reader (typecast/datatype->reader :int8 src-tens true)
          r-ecount (dtype/ecount reader)]
      (dotimes [idx r-ecount]
        (.read reader idx)))))


(defn typed-buffer-read-time-test
  []
  (let [n-elems (* 256 256 4)
        buffer (dtype/make-container :typed-buffer :uint8 n-elems)]
    (time
     (dotimes [iter 1000]
       (let [reader (typecast/datatype->reader :int8 buffer true)
             r-ecount (dtype/ecount reader)]
         (dotimes [idx r-ecount]
           (.read reader idx)))))))


(comment
  (strided-tensor-copy-time-test)
  )
