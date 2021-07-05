(ns tech.v3.datatype.wavelet
  "Implementation of the scipy continuous wavelet transform.  See fastmath for
  implementations of the discrete wavelet transform."
  (:require [tech.v3.datatype.convolve :as dt-conv]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.base :as dt-base]
            [tech.v3.tensor :as dtt]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype DoubleReader]))

(set! *warn-on-reflection* true)


(defn ricker
  ([points a options]
   (let [points (long points)
         a (double a)
         A (/ 2.0  (* (Math/sqrt (* 3 a)) (Math/pow Math/PI 0.25)))
         wsq (* a a)
         offset (/ (- points 1.0) 2.0)]
     (reify DoubleReader
       (lsize [rdr] points)
       (readDouble [rdr idx]
         (let [vec (pmath/- (double idx) offset)
               xsq (* vec vec)
               mod (pmath/- 1.0 (pmath// xsq wsq))
               gauss (Math/exp (pmath// (pmath/- 0.0 xsq) (pmath/* 2.0 wsq)))]
           (pmath/* (pmath/* A mod) gauss))))))
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
                ;;correlate doesn' involve reversing the window
                (dt-conv/correlate1d data window (merge {:mode :same} options))))
            widths)
      (dtt/->tensor :datatype :float64))))
  ([data wavelet-fn widths]
   (cwt data wavelet-fn widths nil)))


(comment
  (require '[tech.viz.vega :as vega]
           '[tech.viz.pyplot :as pyplot])

  (-> (pyplot/figure {:figsize [6 4.5]})
      (pyplot/plot (range (* 5 240)) (ricker (* 5 240) (/ (* 5 240) 5)))
      (pyplot/show))
  )
