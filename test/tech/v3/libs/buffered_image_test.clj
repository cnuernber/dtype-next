(ns tech.v3.libs.buffered-image-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.tensor :as dtt]
            [tech.v3.libs.buffered-image :as bufimg]
            [clojure.test :refer :all]))


(deftest base-img-tensor-test
  (let [test-img (bufimg/new-image 4 4 :int-rgb)
        test-writer (bufimg/as-ubyte-tensor test-img)]
    (.ndWriteLong test-writer 2 2 2 255)
    (is (= [0 0 0 0 0 0 0 0 0 0 16711680 0 0 0 0 0]
           (dtype/->vector test-img)))))


(deftest tensor-and-draw-image
  (let [test-img (bufimg/load "test/data/test.jpg")
        new-img (bufimg/new-image 512 512 :int-argb)
        _ (bufimg/draw-image! test-img new-img :dst-y-offset 128)
        tens-img (bufimg/new-image 512 512 :int-argb)

        copy-tens (dtt/select (bufimg/as-ubyte-tensor tens-img)
                              (range 128 (+ 128 (.getHeight test-img)))
                              :all
                              [0 1 2])
        alpha-tens (dtt/select (bufimg/as-ubyte-tensor tens-img)
                              (range 128 (+ 128 (.getHeight test-img)))
                              :all
                              3)
        _ (is (= (dtype/shape copy-tens)
                 (dtype/shape test-img)))
        _ (dtype/copy! test-img copy-tens)
        _ (dtype/set-constant! alpha-tens 255)]
    (is (dfn/equals (mapv (comp long dfn/mean) [new-img tens-img])
                    [-5240767 -5240767]))))


(deftest channel-order
  (let [test-img (bufimg/load "test/data/test.jpg")
        [height width _n-chan] (dtype/shape test-img)
        channel-fn
        (fn [img-fmt]
          (let [new-img (bufimg/new-image height width img-fmt)
                _ (bufimg/draw-image! test-img new-img)
                first-pix (-> (bufimg/as-ubyte-tensor new-img)
                              (dtt/select 0 0 :all))
                chan-map (bufimg/image-channel-map new-img)]
            (->> [:r :g :b :a]
                 (mapv (fn [chan-name]
                         (if (contains? chan-map chan-name)
                           (dtype/get-value first-pix
                                            (get chan-map chan-name))
                           255))))))
        img-types [:byte-bgr :byte-abgr
                   :int-argb :int-bgr :int-rgb]]
    (doseq [img-type img-types]
      (is (= [170 170 172 255]
             (channel-fn img-type))
          (format "Format %s failed channel order test" img-type)))))
