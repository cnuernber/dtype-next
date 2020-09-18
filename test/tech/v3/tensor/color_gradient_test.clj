(ns tech.v2.tensor.color-gradient-test
  (:require [tech.v2.tensor :as dtt]
            [tech.v2.tensor.color-gradients :as cgrad]
            [tech.v2.datatype.functional :as dfn]
            [clojure.test :refer :all]))


;;Sometimes data has NAN's or INF's
(def test-nan-tens (dtt/->tensor (repeat 128
                                         (->> (range 0 512)
                                              (map-indexed (fn [idx val]
                                                             (if (even? idx)
                                                               Double/NaN
                                                               (double val))))
                                              vec))))


(deftest color-grad-test
  (is (dfn/equals (dtt/->tensor [[  0  0  0   0]
                                 [255 42 34 209]
                                 [  0  0  0   0]
                                 [255 42 34 209]])
                  (-> (cgrad/colorize test-nan-tens :temperature-map
                                      :alpha? true
                                      :check-invalid? true
                                      :invert-gradient? true
                                      :data-min 0
                                      :data-max 512)
                      (dtt/select 0 (range 4) :all)))))


(deftest colorize->clj-test
  (is (= [[[32 252 0] [21 255 25] [0 255 185] [0 225 251] [0 95 255] [0 0 255]]
          [[32 252 0] [21 255 25] [0 255 185] [0 225 251] [0 95 255] [0 0 255]]]
         (cgrad/colorize->clj [[0.0 0.2 0.4 0.6 0.8 1.0]
                               [0.0 0.2 0.4 0.6 0.8 1.0]]
                              :green-red))))
