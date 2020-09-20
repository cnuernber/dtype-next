(ns tech.v3.tensor.select-test
  (:require [tech.v3.tensor :as tens]
            [tech.v3.datatype :as dtype]
            [clojure.test :refer :all]))


(defn ->jvm-flatten
  [tensor]
  (-> (tens/->jvm tensor :datatype :int32)
      flatten))


(deftest select-test
  (let [mat-tens (tens/->tensor (repeat 2 (partition 3 (range 9)))
                                :datatype :int32)]
    (let [sel-tens (tens/select mat-tens :all :all [1 2])]
      (is (= (vec (flatten (repeat 2 [1 2 4 5 7 8])))
             (->jvm-flatten sel-tens)))
      (is (= [2 3 2]
             (dtype/shape sel-tens))))
    (let [sel-tens (tens/select mat-tens :all :all [2])]
      (is (= (flatten (repeat 2 [2 5 8]))
             (->jvm-flatten sel-tens)))
      (is (= [2 3 1]
             (dtype/shape sel-tens))))
    (let [sel-tens (tens/select mat-tens :all :all 2)]
      (is (= (flatten (repeat 2 [2 5 8]))
             (->jvm-flatten sel-tens)))
      (is (= [2 3]
             (dtype/shape sel-tens)))
      (is (not (tens/dimensions-dense? sel-tens))))
    (let [sel-tens (tens/select mat-tens :all [1 2] :all)]
      (is (= (flatten (repeat 2 [3 4 5 6 7 8]))
             (->jvm-flatten sel-tens)))
      (is (= [2 2 3]
             (dtype/shape sel-tens)))
      (is (not (tens/dimensions-dense? sel-tens))))
    (let [sel-tens (tens/select mat-tens :all [2] :all)]
      (is (= (flatten (repeat 2 [6 7 8]))
             (->jvm-flatten sel-tens)))
      (is (= [2 1 3]
             (dtype/shape sel-tens)))
      (is (not (tens/dimensions-dense? sel-tens))))
    (let [sel-tens (tens/select mat-tens :all 0 :all)]
      (is (= (flatten (repeat 2 [0 1 2]))
             (->jvm-flatten sel-tens)))
      (is (= [2 3]
             (dtype/shape sel-tens)))
      (is (not (tens/dimensions-dense? sel-tens))))

    (let [sel-tens (tens/select mat-tens [1] [1] :all)]
      (is (= [3 4 5]
             (->jvm-flatten sel-tens)))
      (is (= [1 1 3]
             (dtype/shape sel-tens)))
      (is (tens/dimensions-dense? sel-tens)))

    (let [sel-tens (tens/select mat-tens 1 1 :all)]
      (is (= [3 4 5]
             (->jvm-flatten sel-tens)))
      (is (= [3]
             (dtype/shape sel-tens)))
      (is (tens/dimensions-dense? sel-tens)))

    (let [sel-tens (tens/select mat-tens 1 :all 2)]
      (is (= [2 5 8]
             (->jvm-flatten sel-tens)))
      (is (= [3]
             (dtype/shape sel-tens)))
      (is (not (tens/dimensions-dense? sel-tens))))))
