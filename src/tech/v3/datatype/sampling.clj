(ns tech.v3.datatype.sampling
  "Implementation of reservoir sampling designed to be used in other systems.  Provides
  a low-level sampler object and a double-reservoir that implements DoubleConsumer and
  derefs to a buffer of doubles."
  (:require [tech.v3.datatype.array-buffer :as abuf]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.packing :as packing]
            [ham-fisted.protocols :as hamf-proto]
            [ham-fisted.api :as hamf])
  (:import [java.util.function LongSupplier Consumer LongConsumer DoubleConsumer]
           [java.util Random]
           [org.apache.commons.math3.random MersenneTwister]
           [ham_fisted IMutList Reducible]
           [clojure.lang IDeref IObj]))


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
  (getAsLong [_this]
    (set! n-elems-seen (unchecked-inc n-elems-seen))
    (if (== n-skip 0)
      (do
        (set! n-skip (next-n-skip random w))
        (set! w (* w (exp-log-random-over-k random reservoir-size)))
        (.nextInt random (int reservoir-size)))
      (do (set! n-skip (unchecked-dec n-skip))
          -1)))
  IObj
  (withMeta [_this metadata]
    (ReservoirSampler. n-elems-seen n-skip reservoir-size random w metadata))
  (meta [_this] metadata)
  IDeref
  (deref [_this]
    {:n-elems-seen n-elems-seen
     :n-skip n-skip
     :reservoir-size reservoir-size
     :random random
     :w w}))


(defn reservoir-sampler-supplier
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
   (reservoir-sampler-supplier reservoir-size nil)))


(deftype ObjectRes [^IMutList container
                    ^long reservoir-size
                    ^LongSupplier sampler]
  Consumer
  (accept [this val]
    (if (< (.size container) reservoir-size)
      (.add container val)
      (let [replace-idx (.getAsLong sampler)]
        (when (>= replace-idx 0)
          (.set container replace-idx val)))))
  Reducible
  (reduce [this other]
    (reduce hamf/consumer-accumulator this @other))
  IDeref
  (deref [this] container))


(deftype LongRes [^IMutList container
                  ^long reservoir-size
                  ^LongSupplier sampler]
  LongConsumer
  (accept [this val]
    (if (< (.size container) reservoir-size)
      (.addLong container val)
      (let [replace-idx (.getAsLong sampler)]
        (when (>= replace-idx 0)
          (.setLong container replace-idx val)))))
  Reducible
  (reduce [this other]
    (reduce hamf/long-consumer-accumulator this @other))
  IDeref
  (deref [this] container))


(deftype DoubleRes [^IMutList container
                    ^long reservoir-size
                    ^LongSupplier sampler]
  DoubleConsumer
  (accept [this val]
    (if (< (.size container) reservoir-size)
      (.addDouble container val)
      (let [replace-idx (.getAsLong sampler)]
        (when (>= replace-idx 0)
          (.setDouble container replace-idx val)))))
  Reducible
  (reduce [this other]
    (reduce hamf/double-consumer-accumulator this @other))
  IDeref
  (deref [this] container))


(defn reservoir-sampler
    "Return hamf parallel reducer that will accept objects and whose value is the
  reservoir of data.

  Merging consists of adding elements from the second distribution into the first.

  Same options as ->random and the options are passed unchanged into make-container:

  * `:algorithm` - either `:lcg` or `:mersenne-twister`. Defaults to `:lcg` which is
     the default java implementation.
  * `:seed` - long integer seed.
  * `:random` - User provided instance of java.util.Random.  If this exists it
     overrides all other options.
  * `:datatype` - Specify container's datatype.  Defaults to :object

  Examples:

```clojure
tech.v3.datatype.sampling> (hamf/reduce-reducer (reservoir-sampler 10) (range 200))
[189 15 49 128 167 157 170 7 182 162]
tech.v3.datatype.sampling> (hamf/reduce-reducer (reservoir-sampler 10 {:datatype :float32}) (range 200))
[0.0 117.0 37.0 3.0 190.0 186.0 27.0 89.0 63.0 108.0]
tech.v3.datatype.sampling> (hamf/preduce-reducer (reservoir-sampler 10 {:datatype :float32}) (range 200000))
[5750.0
 128996.0
 146881.0
 174104.0
 101110.0
 24560.0
 25344.0
 170374.0
 158145.0
 124138.0]
```
  "
  ([reservoir-size] (reservoir-sampler reservoir-size nil))
  ([^long reservoir-size options]
   (let [dtype (get options :datatype :object)]
     (case (casting/simple-operation-space (packing/unpack-datatype
                                            (get options :datatype :object)))
       :int64
       (reify
         hamf-proto/Reducer
         (->init-val-fn [s] #(LongRes. (abuf/array-list dtype)
                                       reservoir-size
                                       (reservoir-sampler-supplier reservoir-size options)))
         (->rfn [s] hamf/long-consumer-accumulator)
         hamf-proto/Finalize
         (finalize [s v] (deref v))
         hamf-proto/ParallelReducer
         (->merge-fn [this] hamf/reducible-merge))
       :float64
       (reify
         hamf-proto/Reducer
         (->init-val-fn [s] #(DoubleRes. (abuf/array-list dtype)
                                         reservoir-size
                                         (reservoir-sampler-supplier reservoir-size options)))
         (->rfn [s] hamf/double-consumer-accumulator)
         hamf-proto/Finalize
         (finalize [s v] (deref v))
         hamf-proto/ParallelReducer
         (->merge-fn [this] hamf/reducible-merge))
       (reify
         hamf-proto/Reducer
         (->init-val-fn [s] #(ObjectRes. (abuf/array-list dtype)
                                         reservoir-size
                                         (reservoir-sampler-supplier reservoir-size options)))
         (->rfn [s] hamf/consumer-accumulator)
         hamf-proto/Finalize
         (finalize [s v] (deref v))
         hamf-proto/ParallelReducer
         (->merge-fn [this] hamf/reducible-merge))))))
