(ns tech.v3.datatype.convolve
  "Namespace for implementing various basic convolutions.  Currently only 1d
  convolutions are supported."
  (:require [tech.v3.datatype.base :as dt-base]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.copy-make-container :as dt-cmc]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.parallel.for :as pfor]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype Convolve1D Convolve1D$Mode ArrayHelpers DoubleReader
            Convolve1D$EdgeMode Convolve1D$Edging Complex]
           [java.util.function BiFunction]
           [java.util Arrays]
           [org.jtransforms.fft DoubleFFT_1D]))

(defn- edge-mode->edging
  ^Convolve1D$Edging [edge-mode]
  (cond
    (keyword? edge-mode )
    (Convolve1D$Edging.
     (case edge-mode
       :zero Convolve1D$EdgeMode/Zero
       :clamp Convolve1D$EdgeMode/Clamp
       :reflect Convolve1D$EdgeMode/Reflect
       :wrap Convolve1D$EdgeMode/Wrap
       :nearest Convolve1D$EdgeMode/Nearest))
    (number? edge-mode)
    (Convolve1D$Edging. Convolve1D$EdgeMode/Constant (double edge-mode))
    :else
    (errors/throwf "Unrecognized edge mode: %s" edge-mode)))


(defn- mode->conv-mode
  ^Convolve1D$Mode [mode]
  (case mode
    :valid Convolve1D$Mode/Valid
    :same Convolve1D$Mode/Same
    :full Convolve1D$Mode/Full))


(defn- next-pow-2
  ^long [^long val]
  (loop [retval 1]
    (if (pmath/< retval val)
      (recur (bit-shift-left retval 1))
      retval)))


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
         virt-sig (-> (edge-mode->edging edge-mode)
                      (.apply signal filter-len (mode->conv-mode mode)))
         virt-sig-len (alength virt-sig)
         fft-win-offset (dec filter-len)
         ;;Multiply filter by 2 to respect nyquist frequency
         fft-default-size (min 2048 (next-pow-2 signal-len))
         fft-size (long (or fft-size (max fft-default-size
                                          (next-pow-2 (* 2 filter-len)))))
         ;;Amount of each fft window is occupied by signal.  A huge question
         ;;I have is why is this fft-size - filter-len + 1?  Where did that
         ;;come from?
         fft-signal-size (inc (- fft-size filter-len))
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
         fft-result (double-array fft-ary-size)]
     (dotimes [window-idx n-windows]
       (let [win-off (* window-idx fft-signal-size)
             win-end (min virt-sig-len (+ win-off fft-signal-size))
             win-len (- win-end win-off)]
         ;;set complex parts to zero
         (Arrays/fill signal-input 0.0)
         ;;fill left edge constant values
         (Complex/realToComplex virt-sig win-off signal-input 0 win-len)
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

  * `:mode` - defaults to `:full` - one of:
    - :full - Result size is ndata + 2 * (nwin - 1)
    - :same - Result size is ndata
    - :valid - Result size is ndata - nwin + 1
  * `:edge-mode` - defaults to `:zero`- one of:
    - `:zero` - pad zero
    - `:clamp` - Clamp to edge values.
    - `:nearest` - Same as clamp, python compat.
    - `:reflect` - reflect the data - abcddcba|abcd|dcbaabcd
    - `:wrap` - wrap data repeating - abcdabcd|abcd|abcdabcd
    - number - pad with constant number.
  * `:force-serial` - For serial execution of the naive convolution.  Unnecessary except
  for profiling and comparison purposes.
  * `:stepsize` - Defaults to 1, this steps across the input data in stepsize
    increments.  `:fft` alorithm cannot be used if stepsize is not 1.
  * `:algorithm` - `:naive`, `:auto`, or `:fft`.  Defaults to `:auto` which will choose
     either `:naive` or `:fft` depending on input data size.
```"
  ([data win {:keys [mode force-serial edge-mode stepsize algorithm]
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
            (case mode
              :full Convolve1D$Mode/Full
              :same Convolve1D$Mode/Same
              :valid Convolve1D$Mode/Valid)
            (edge-mode->edging edge-mode))
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


(defn ^:no-doc gauss-kernel-1d
  "Create a 1d gaussian kernel.
  TODO - add in order.  This requires code located at:

  https://github.com/scipy/scipy/blob/v1.6.3/scipy/ndimage/filters.py#L193"
  [sigma radius]
  (let [sigma (double sigma)
        sigma2 (* sigma sigma)
        radius (long radius)
        x (range (- radius) (inc radius))
        phi-x (dfn/exp (dfn/* (dfn// -0.5 sigma2) (dfn/pow x 2)))]
    (dfn// phi-x (dfn/sum phi-x))))


(defn gaussian1d
  "1-D Gaussian filter.

  data : convertible to reader.
  sigma : scalar - standard deviation for Gaussian kernel.  See `:truncate`
    for how this relates to window width.

  Options:

  * `:mode` - defaults to `:same`, see options for correlate1d.
  * `:edge-mode` - defaults to `:reflect`, see options for correlate1d.
  * `:truncate` - Defaults to 4.  Truncate the filter at this
     many standard deviations.  Convolution window is equal to
     `(long (+ (* truncate sigma) 0.5))`"
  ([data sigma {:keys [truncate mode edge-mode]
                     :or {truncate 4
                          mode :same
                          edge-mode :reflect}
                :as options}]
   ;; make the radius of the filter equal to truncate standard deviations
   (let [lw (long (+ (* truncate sigma) 0.5))
         window (gauss-kernel-1d sigma lw)]
     (correlate1d data window (assoc options :mode mode :edge-mode edge-mode))))
  ([data window-len]
   (gaussian1d data window-len nil)))


(comment

  (convolve1d [1, 2, 3], [0, 1, 0.5])
  (convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :same})
  (convolve1d [1, 2, 3], [0, 1, 0.5] {:mode :valid})
  (convolve1d (range 100), [0, 1, 0.5] {:edge-mode :zero
                                        :stepsize 2})

  (gaussian1d [1.0, 2.0, 3.0, 4.0, 5.0] 1 {:edge-mode :reflect})

  (do
    (import '[tech.v3.datatype Convolve1D Convolve1D$EdgeMode
              Convolve1D$Edging])

    (vec (.apply (Convolve1D$Edging. Convolve1D$EdgeMode/Reflect)
                 (double-array (range 5)) 11)))

  (vec (.apply (Convolve1D$Edging. Convolve1D$EdgeMode/Constant 1)
               (double-array (range 5)) 10))

  (vec (.apply (Convolve1D$Edging. Convolve1D$EdgeMode/Wrap 0)
               (double-array (range 1 5)) 15))


  )
