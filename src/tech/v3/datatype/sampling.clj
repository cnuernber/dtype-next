(ns tech.v3.datatype.sampling
  "Implementation of reservoir sampling designed to be used in other systems.  Provides
  a low-level sampler object and a double-reservoir that implements DoubleConsumer and
  derefs to a buffer of doubles."
  (:require [tech.v3.datatype.list :as dtype-list]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc])
  (:import [java.util.function LongSupplier DoubleToLongFunction DoubleConsumer]
           [java.util Random]
           [org.apache.commons.math3.random MersenneTwister]
           [clojure.lang IDeref IObj]
           [tech.v3.datatype Consumers$StagedConsumer PrimitiveList]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ->random
  "Given an options map return an implementation of java.util.Random.

  Options:

  * `:algorithm` - either `:lcg` or `:mersenne-twister`. Defaults to `:lcg` which is
     the default java implementation.
  * `:seed` - long integer seed.
  * `:random` - User provided instance of java.util.Random.  If this exists it
     overrides all other options."
  (^Random [{:keys [random seed algorithm]
             :or {algorithm :lcg}}]
   (if (instance? Random random)
     random
     (case algorithm
       :lcg (if seed (Random. (long seed)) (Random.))
       :mersenne-twister (if seed (MersenneTwister. (long seed)) (MersenneTwister.)))))
  (^Random []
   (->random nil)))


(defn- exp-log-random-over-k
  ^double [^Random random ^long k]
  (Math/exp (/ (Math/log (.nextDouble random))
               (double k))))


(defn- next-n-skip
  ^long [^Random random ^double w]
  (unchecked-inc
   (long
    (Math/floor (/ (Math/log (.nextDouble random))
                   (Math/log (- 1.0 w)))))))


(deftype ReservoirSampler [^{:unsynchronized-mutable true
                             :tag long} n-elems-seen
                           ^{:unsynchronized-mutable true
                             :tag long} n-skip
                           ;; k is reservoir-size
                           ^int reservoir-size
                           ^Random random
                           ;;wikipedia naming...
                           ^{:unsynchronized-mutable true
                             :tag double} w
                           metadata]
  LongSupplier
  (getAsLong [this]
    (set! n-elems-seen (unchecked-inc n-elems-seen))
    (if (== n-skip 0)
      (do
        (set! n-skip (next-n-skip random w))
        (set! w (* w (exp-log-random-over-k random reservoir-size)))
        (.nextInt random (int reservoir-size)))
      (do (set! n-skip (unchecked-dec n-skip))
          -1)))
  IObj
  (withMeta [this metadata]
    (ReservoirSampler. n-elems-seen n-skip reservoir-size random w metadata))
  (meta [this] metadata)
  IDeref
  (deref [this]
    {:n-elems-seen n-elems-seen
     :n-skip n-skip
     :reservoir-size reservoir-size
     :random random
     :w w}))


(defn reservoir-sampler
  "Create a `java.util.function.LongSupplier` that will generate an infinite sequence
  of longs.  If a long is -1 it means do nothing, else it will output the index
  to replace with the new item.  The sampler expects the reservoir is full before
  the first call to `.getAsLong`.

  * [optimal reservoir algorithm](https://dl.acm.org/doi/10.1145/198429.198435).

  deref'ing the supplier allows you to see the internal state of the sampler.

  Same options as ->random:

  * `:algorithm` - either `:lcg` or `:mersenne-twister`. Defaults to `:lcg` which is
     the default java implementation.
  * `:seed` - long integer seed.
  * `:random` - User provided instance of java.util.Random.  If this exists it
     overrides all other options."
  (^LongSupplier [reservoir-size options]
   (let [random (->random options)
         reservoir-size (long reservoir-size)
         w (exp-log-random-over-k random reservoir-size)
         n-skip (next-n-skip random w)]
     (ReservoirSampler. 0 n-skip (int reservoir-size) random w nil)))
  (^LongSupplier [reservoir-size]
   (reservoir-sampler reservoir-size nil)))



(defn double-reservoir
  "Return a staged double consumer that will accept doubles and whose value is the
  reservoir of data.

  Merging consists of adding elements from the second distribution into the first.

  Same options as ->random and the options are passed unchanged into make-container:

  * `:algorithm` - either `:lcg` or `:mersenne-twister`. Defaults to `:lcg` which is
     the default java implementation.
  * `:seed` - long integer seed.
  * `:random` - User provided instance of java.util.Random.  If this exists it
     overrides all other options.
  * `:container-type` - Specify container type, defaults to `:jvm-heap`."
  (^DoubleConsumer [reservoir-size options]
   (let [reservoir-size (long reservoir-size)
         container (dtype-cmc/make-container (:container-type options :jvm-heap)
                                             :float64 options reservoir-size)
         ^PrimitiveList reservoir (dtype-list/make-list container 0)
         sampler (reservoir-sampler reservoir-size options)]
     (reify
       DoubleConsumer
       (accept [this val]
         (if (< (.lsize reservoir) reservoir-size)
           (.add reservoir val)
           (let [replace-idx (.getAsLong sampler)]
             (when (>= replace-idx 0)
               (.writeDouble reservoir replace-idx val)))))
       Consumers$StagedConsumer
       (combine [this other]
         (let [other-data (dtype-base/->buffer (.value other))]
           (dotimes [idx (.lsize other-data)]
             (.accept this (.readDouble other-data idx)))
           this))
       (value [this]
         reservoir))))
  (^DoubleConsumer [reservoir-size]
   (double-reservoir reservoir-size nil)))
