(ns tech.v3.datatype.wavelet
  "Implementation of the scipy continuous wavelet transform.  See fastmath for
  implementations of the discrete wavelet transform."
  (:require [tech.v3.datatype.convolve :as dt-conv]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.tensor :as dtt]))


(defn ricker
  ([points a options]
   (let [points (long points)
         a (double a)
         A (/ 2.0  (* (Math/sqrt (* 3 a)) (Math/pow Math/PI 0.25)))
         wsq (* a a)
         vec (dfn/- (range 0 points) (/ (- points 1.0) 2))
         xsq (dfn/sq vec)
         mod (dfn/- 1  (dfn// xsq wsq))
         gauss (dfn/exp (dfn// (dfn/- 0 xsq) (dfn/* 2 wsq)))]
     (dfn/* (dfn/* A mod) gauss)))
  ([points a]
   (ricker points a nil)))


(defn cwt
  "Minimal version of scipy's cwt.  Only real datatypes are supported."
  ([data wavelet-fn widths options]
   (let [dlen (dt-base/ecount data)]
     (->
      (pmap (fn [width]
              (let [width (double width)
                    N (long (Math/min (* 10 width) (double dlen)))
                    window (wavelet-fn N width options)]
                (dt-conv/convolve1d data window (merge {:mode :same}options))))
            widths)
      (dtt/->tensor :datatype :float64))))
  ([data wavelet-fn widths]
   (cwt data wavelet-fn widths nil)))


(comment
  (require '[tech.viz.vega :as vega]
           '[tech.viz.pyplot :as pyplot])

  (-> (pyplot/figure {:figsize [6 4.5]})
      (pyplot/plot (range 100) (ricker 100 4))
      (pyplot/show))
  )
