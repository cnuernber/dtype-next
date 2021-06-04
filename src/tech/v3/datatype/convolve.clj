(ns tech.v3.datatype.convolve
  "Namespace for implementing various basic convolutions.  Currently only 1d
  convolutions are supported."
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.parallel.for :as pfor]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype Convolve1D Convolve1D$Mode ArrayHelpers DoubleReader
            Convolve1D$EdgeMode Complex]
           [java.util.function BiFunction]
           [java.util Arrays]
           [org.apache.commons.math3.distribution NormalDistribution]
           [org.jtransforms.fft DoubleFFT_1D]))


(defn- next-pow-2
  ^long [^long val]
  (loop [retval 1]
    (if (pmath/< retval val)
      (recur (bit-shift-left retval 1))
      retval)))

(deftype ^:private FFTWindow [^long start ^long end])


(defn- window-len
  ^long [^FFTWindow win]
  (- (.end win) (.start win)))


(defn- window-intersect
  ^FFTWindow [^FFTWindow lhs ^FFTWindow rhs]
  (let [win-off (max (.start lhs) (.start rhs))
        win-end (min (.end lhs) (.end rhs))]
    (when (pmath/> win-end win-off)
      (FFTWindow. win-off win-end))))


(defn ^:no-doc convolve-fft-1d
  ([signal filter {:keys [mode edge-mode fft-size]
                   :or {mode :full
                        edge-mode :zero}}]
   (let [filter (dt-cmc/->double-array filter)
         filter-len (alength filter)
         dec-filter-len (dec filter-len)
         signal (dt-cmc/->double-array signal)
         signal-len (count signal)
         n-result (long (case mode
                          :full (+ signal-len dec-filter-len)
                          :same signal-len
                          :valid (inc (- signal-len filter-len))))
         ;;Because we pad the signal, we have a virtual coordinate space
         ;;for the padded signal.
         [sig-virt-start sig-virt-end]
         (case mode
           :full [(- dec-filter-len)
                  (+ signal-len dec-filter-len)]
           :same [(- (quot dec-filter-len 2))
                  (+ signal-len (quot dec-filter-len 2))]
           :valid [0 signal-len])

         sig-virt-start (long sig-virt-start)
         sig-virt-end (long sig-virt-end)
         virt-sig-len (- sig-virt-end sig-virt-start)

         [sig-left-edge sig-right-edge]
         (case edge-mode
           :zero [0.0 0.0]
           :clamp [(aget signal 0) (aget signal (dec signal-len))])

         sig-left-edge (double sig-left-edge)
         sig-right-edge (double sig-right-edge)
         fft-win-offset (long
                         (case mode
                           :same (quot filter-len 2)
                           :full 0
                           :valid (dec filter-len)))
         ;;Multiply filter by 2 to respect nyquist frequency
         fft-default-size (min 2048 (next-pow-2 signal-len))
         fft-size (long (or fft-size (max fft-default-size
                                          (next-pow-2 (* 2 filter-len)))))
         ;;Amount of each fft window is occupied by signal
         fft-signal-size fft-size
         ;;Size of a double array that can be used in complexForward/Inverse.
         fft-ary-size (* 2 fft-size)
         ;;Work on same size transfer for step 1.
         result (double-array n-result)
         ;;Number of windows to execute
         n-windows (quot (+ virt-sig-len (dec fft-signal-size)) fft-signal-size)
         fft (DoubleFFT_1D. fft-size)
         filter-fft (let [fft-input (Complex/realToComplex
                                     filter 0
                                     (double-array fft-ary-size) 0
                                     filter-len)]
                      (.complexForward fft fft-input)
                      fft-input)
         signal-input (double-array fft-ary-size)
         fft-result (double-array fft-ary-size)
         lhs-window (FFTWindow. sig-virt-start 0)
         center-window (FFTWindow. 0 signal-len)
         rhs-window (FFTWindow. signal-len sig-virt-end)]
     (dotimes [window-idx n-windows]
       (let [win-off (+ (* window-idx fft-signal-size) sig-virt-start)
             win-end (min sig-virt-end (+ win-off fft-signal-size))
             cur-window (FFTWindow. win-off win-end)
             valid-lhs (window-intersect lhs-window cur-window)
             lhs-win-len (long (if valid-lhs (window-len valid-lhs) 0))
             valid-cen (window-intersect center-window cur-window)
             cen-win-len (long (if valid-cen (window-len valid-cen) 0))
             valid-rhs (window-intersect rhs-window cur-window)]
         ;;set complex parts to zero
         (Arrays/fill signal-input 0.0)
         ;;fill left edge constant values
         (when valid-lhs
           (Complex/realToComplex sig-left-edge signal-input 0 (window-len valid-lhs)))
         (when valid-cen
           (Complex/realToComplex signal (.start valid-cen) signal-input lhs-win-len
                                  (window-len valid-cen)))
         (when valid-rhs
           (Complex/realToComplex sig-right-edge signal-input (+ lhs-win-len
                                                                 cen-win-len)
                                  (window-len valid-rhs)))
         (.complexForward fft signal-input)
         (Complex/mul signal-input 0 filter-fft 0 fft-result 0 fft-size)
         (.complexInverse fft fft-result true)
         (dotimes [vidx fft-size]
           (let [win-idx (- (+ vidx win-off) fft-win-offset)]
             (when (and (>= win-idx 0)
                        (< win-idx n-result))
               (ArrayHelpers/accumPlus result win-idx
                                       (aget fft-result (* 2 vidx))))))))
     (array-buffer/array-buffer result)))
  ([signal filter]
   (convolve-fft-1d signal filter nil)))


(defn correlate1d
  "Convolve window over data.  Window must be smaller than data.
  Returns result in `:float64` space.  Convolutions outside the valid
  space of data are supplied zeros to match numpy conventions.

  Options:

  * `:mode` - One of :full, :same, or :valid.
  * `:edge-mode` - One of :zero or :clamp.  Clamp clamps the edge value to the start
    or end respectively. Defaults to :zero.
  * `:finalizer` - A java.util.function.DoubleUnaryOperator that performs
  one more operation on the data after summation but before assigned to
  result.
  * `:force-serial` - For serial execution of the convolution.  Unnecessary except
  for profiling and comparison purposes.
  * `:stepsize` - Defaults to 1, this steps across the input data in stepsize
    increments.  `:fft` alorithm cannot be used if stepsize is not 1.
  * `:algorithm` - `:naive`, `:auto`, or `:fft`.  Defaults to `:auto` which will choose
     either `:naive` or `:fft` depending on input data size.
```"
  ([data win {:keys [mode finalizer force-serial edge-mode stepsize algorithm]
              :or {mode :full
                   edge-mode :zero
                   stepsize 1}
              :as options}]
   (let [data (dt-cmc/->double-array data)
         win (dt-cmc/->double-array win)
         n-data (* (alength data) (alength win))
         algorithm (if (== 1 stepsize)
                     (or algorithm
                         (if (< n-data 64000)
                           :naive
                           :fft))
                     :naive)]
     (case algorithm
       :fft
       (let [win-len (alength win)
             dec-win-len (dec win-len)]
         (convolve-fft-1d data (reify DoubleReader
                                 (lsize [this] win-len)
                                 (readDouble [this idx]
                                   (aget win (- dec-win-len idx))))
                          (assoc options :mode mode :edge-mode edge-mode)))
       :naive
       (-> (Convolve1D/correlate
            (reify BiFunction
              (apply [this n-elems applier]
                (if force-serial
                  (.apply ^BiFunction applier 0 n-elems)
                  (pfor/indexed-map-reduce
                   n-elems
                   (fn [start-idx group-len]
                     (.apply ^BiFunction applier start-idx group-len))
                   dorun))))
            data
            win
            (int stepsize)
            finalizer
            (case mode
              :full Convolve1D$Mode/Full
              :same Convolve1D$Mode/Same
              :valid Convolve1D$Mode/Valid)
            (case edge-mode
              :zero Convolve1D$EdgeMode/Zero
              :clamp Convolve1D$EdgeMode/Clamp))
           (array-buffer/array-buffer)))))
  ([data win]
   (correlate1d data win nil)))


(defn convolve1d
  "Convolve a window across a signal.  The only difference from correlate is the
  window is reversed and then correlate is called.  See options for correlate, this
  has the same defaults.

  Examples:

```clojure
user> (require '[tech.v3.datatype.convolve :as dt-conv])
nil
user> (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5])
#array-buffer<float64>[5]
[0.000, 1.000, 2.500, 4.000, 1.500]
user> (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :same})
#array-buffer<float64>[3]
[1.000, 2.500, 4.000]
user> (dt-conv/convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :valid})
#array-buffer<float64>[1]
[2.500]
```"
  ([data win options]
   (let [win (dt-base/->buffer win)
         n-win (.lsize win)
         n-win-dec (dec n-win)]
     (correlate1d data
                  (reify DoubleReader
                    (lsize [this] n-win)
                    (readDouble [this idx]
                      (.readDouble win (- n-win-dec idx))))
                  options)))
  ([data win]
   (convolve1d data win nil)))


(defn gaussian1d
  "Perform a 1d gaussian convolution.  Options are passed to convolve1d - but the
  default mode is `:same` as opposed to `:full`.  Values are drawn from a 0 centered
  normal distribution with stddev of 1 on the range of -range*sigma,range*sigma at
  window-len increments. Then the window vector is normalized to length 1 and convolved
  over the data using correlate1d.  This means with a range of '2' you are drawing
  from a 0-centered normal distribution 2 standard distributions so the edges will have
  a pre-normalized value of around 0.05.

  Options:

  * `:mode` - defaults to `:same`, see options for correlate1d.
  * `:edge-mode` - defaults to `:clamp`, see options for correlate1d.
  * `:range` - Defaults to 2, values are drawn from (-range, range) with
    window-len increments.
  * `:stepsize` - Defaults to 1.  Increasing this decreases the size of the
    result by a factor of stepsize.  So you can efficiently downsample your data
    by using a gaussian window and increasing stepsize to the desired downsample
    amount."
  ([data window-len {:keys [range mode edge-mode]
                     :or {range 2
                          mode :same
                          edge-mode :clamp}
                     :as options}]
   (let [dist (NormalDistribution.)
         range (double range)
         window-len (long window-len)
         total-range (* 2 (double range))
         increment (/ total-range (dec window-len))
         window (double-array window-len)
         range-start (double (- range))
         sum
         (loop [idx 0
                sum 0.0]
           (let [next-val (+ range-start (* increment idx))
                 next-dist (.density dist next-val)]
             (if (< idx window-len)
               (do
                 (aset window idx next-dist)
                 (recur (unchecked-inc idx)
                        (+ sum (* next-dist next-dist))))
               sum)))
         inv-len (double (if (== sum 0.0)
                           0.0
                           (/ 1.0 (Math/sqrt sum))))]
     ;;Normalize the vector
     (dotimes [idx window-len]
       (ArrayHelpers/accumMul window idx inv-len))
     (correlate1d data window (assoc options :mode mode :edge-mode edge-mode))))
  ([data window-len]
   (gaussian1d data window-len nil)))


(comment

  (convolve1d [1, 2, 3], [0, 1, 0.5])
  (convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :same})
  (convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :valid})
  (convolve1d (range 100), [0, 1, 0.5] {:edge-mode :zero
                                        :stepsize 2})
  )
