(ns tech.v3.datatype.convolve-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.convolve :as dt-conv]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))



(deftest basetest
  (is (dfn/equals [0.000, 1.000, 2.500, 4.000, 1.500]
                  (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5])))
  (is (dfn/equals [0.000, 1.000, 2.500, 4.000, 1.500]
                  (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5]
                                      {:algorithm :fft})))
  (is (dfn/equals [1 2.5 4]
                  (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :same})))
  (is (dfn/equals [1 2.5 4]
                  (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :same
                                                              :algorithm :fft})))

  (is (dfn/equals [2.5]
                  (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :valid})))
  (is (dfn/equals [2.5]
                  (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :valid
                                                              :algorithm :fft})))

  (let [src-data (dfn/sin (range 0 20 0.1))
        modes [:same :valid :full]
        edge-modes [:zero :clamp]
        window-sizes (range 5 15)]
    (->> (for [mode modes
               edge-mode edge-modes
               window-size window-sizes]
           (is (dfn/equals (dt-conv/convolve1d src-data (range window-size)
                                               {:mode mode :edge-mode edge-mode})
                           (dt-conv/convolve1d src-data (range window-size)
                                               {:mode mode :edge-mode edge-mode
                                                :algorithm :fft}))
               (format "Algorithm mismatch: mode %s edge-mode %s window-size %d"
                       mode edge-mode window-size)))
         dorun)))
