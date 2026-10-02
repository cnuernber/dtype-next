(ns tech.v3.datatype.convolve-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.convolve :as dt-conv]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))



(deftest basetest
  (is (dfn/equals [0.000 1.000 2.500 4.000 1.500]
                  (dt-conv/convolve1d [1 2 3] [0 1 0.5])))
  (is (dfn/equals [0.000 1.000 2.500 4.000 1.500]
                  (dt-conv/convolve1d [1 2 3] [0 1 0.5]
                                      {:algorithm :fft})))

  (is (dfn/equals [0.000 1.000 2.500 5.000 3.500 3.000]
                  (dt-conv/convolve1d [1 2 3] [0 1 0.5 1])))
  (is (dfn/equals [0.000 1.000 2.500 5.000 3.500 3.000]
                  (dt-conv/convolve1d [1 2 3] [0 1 0.5 1]
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
        edge-modes [:zero :clamp :reflect]
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

(deftest gaussian1d
  (is (dfn/equals [1.42704095 2.06782203 3 3.93217797 4.57295905]
                  (dt-conv/gaussian1d [1.0 2.0 3.0 4.0 5.0] 1)))

  (is (dfn/equals [0.08167442, 0.10164546, 0.11883558, 0.13051536, 0.13465836,
                   0.13051536, 0.11883558, 0.10164546, 0.08167442]
                  (dt-conv/gauss-kernel-1d 4 4)))

  (is (dfn/equals [2.91948343 2.95023502 3. 3.04976498 3.08051657]
                  (dt-conv/gaussian1d [1.0 2.0 3.0 4.0 5.0] 4)))
  )

(comment
  (do
    (def src-data (dfn/sin (range 0 20 0.1)))
    (dt-conv/convolve1d src-data
                        (range 5)
                        {:mode :same
                         :edge-mode :zero
                         :algorithm :fft})
    )
  )


(deftest edge-fill-covers-last-element
  ;;Constant/clamp edging used to leave the final padded element as 0.0
  (is (= [1.0 1.0 1.0 2.0 3.0 3.0 3.0]
         (vec (.apply (tech.v3.datatype.Convolve1D$Edging.
                       tech.v3.datatype.Convolve1D$EdgeMode/Clamp)
                      (double-array [1 2 3]) 7))))
  (is (= [9.0 9.0 1.0 2.0 3.0 9.0 9.0]
         (vec (.apply (tech.v3.datatype.Convolve1D$Edging.
                       tech.v3.datatype.Convolve1D$EdgeMode/Constant 9.0)
                      (double-array [1 2 3]) 7))))
  (is (dfn/equals [3 4 6 8 9]
                  (dt-conv/convolve1d [1 2 3] [1 1 1] {:edge-mode :clamp}))))
