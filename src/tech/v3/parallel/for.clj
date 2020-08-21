(ns tech.v3.parallel.for
  (:import [java.util.concurrent ForkJoinPool Callable Future ExecutorService]
           [java.util ArrayDeque PriorityQueue Comparator]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn indexed-map-reduce
  "Given a function that takes exactly 2 arguments, a start-index and a length,
call this function exactly N times where N is ForkJoinPool/getCommonPoolParallelism.
  Indexes will be split as evenly as possible among the invocations.  Uses
  ForkJoinPool/commonPool for parallelism.  The entire list of results (outputs of
  indexed-map-fn) are passed to reduce-fn; reduce-fn is called once.
  When called with 2 arguments the reduction function is dorun"
  ([^long num-iters indexed-map-fn reduce-fn]
   (if (< num-iters (* 2 (ForkJoinPool/getCommonPoolParallelism)))
     (reduce-fn [(indexed-map-fn 0 num-iters)])
     (let [num-iters (long num-iters)
           parallelism (ForkJoinPool/getCommonPoolParallelism)
           group-size (quot num-iters parallelism)
           overflow (rem num-iters parallelism)
           overflow-size (+ group-size 1)
           ;;Get pairs of (start-idx, len) to launch callables
           common-pool (ForkJoinPool/commonPool)]
       (->> (range parallelism)
            ;;Force start of execution with mapv
            (mapv (fn [^long callable-idx]
                    (let [group-len (if (< callable-idx overflow)
                                      overflow-size
                                      group-size)
                          group-start (+ (* overflow-size
                                            (min overflow callable-idx))
                                         (* group-size
                                            (max 0 (- callable-idx overflow))))
                          callable #(indexed-map-fn group-start group-len)]
                      (.submit common-pool ^Callable callable))))
            (map #(.get ^Future %))
            (reduce-fn)))))
  ([num-iters indexed-map-fn]
   (indexed-map-reduce num-iters indexed-map-fn dorun)))


(defn launch-parallel-for
  "Legacy name.  See indexed-map-reduce"
  ([^long num-iters indexed-map-fn reduce-fn]
   (indexed-map-reduce num-iters indexed-map-fn reduce-fn))
  ([num-iters indexed-map-fn]
   (indexed-map-reduce num-iters indexed-map-fn)))


(defmacro dotimes-batched
  "Uses dotimes under the covers but provides support for a maximum batch size.
  Use this in production code to limit dotimes so you have safepoint access for
  really large iterations"
  [[idx-var n-elems batch-size] & body]
  `(let [batch-size# (int ~batch-size)
         n-elems# (long ~n-elems)
         n-batches# (quot (+ n-elems# (max 0 (unchecked-dec batch-size#)))
                          batch-size#)]
     ;;Doing batches in a doseq so that the JVM doesn't think it is a tight loop.
     ;;This allows the safepoint mechanism in java8 to work correctly.
     (doseq [batch-idx# (range n-batches#)]
       (let [start-idx# (* (long batch-idx#) batch-size#)
             end-idx# (min (+ start-idx# batch-size#) n-elems#)]
         (loop [idx# start-idx#]
           (when (< idx# end-idx#)
             (let [~idx-var idx#]
               ~@body
               (recur (unchecked-inc idx#)))))))))


(defmacro dotimes-safepoint
  "dotimes that allows safepoint access for parallelized loops that could
  get quite large."
  [[idx-var n-elems] & body]
  `(dotimes-batched
    [~idx-var ~n-elems 10000]
    ~@body))


(defmacro serial-for
  "Utility to quickly switch between parallel/serial loop execution"
  [idx-var num-iters & body]
  `(dotimes-batched
    [~idx-var ~num-iters]
    ~@body))

(defmacro parallel-for
  "Like clojure.core.matrix.macros c-for except this expects index that run from 0 ->
  num-iters.  Idx is named idx-var and body will be called for each idx in parallel.
  Uses forkjoinpool's common pool for parallelism.  Assumed to be side effecting; returns
  nil."
  [idx-var num-iters & body]
  `(let [num-iters# (long ~num-iters)]
     (if (< num-iters# (* 2 (ForkJoinPool/getCommonPoolParallelism)))
       (dotimes-safepoint [~idx-var num-iters#]
         ~@body)
       (indexed-map-reduce
        num-iters#
        (fn [^long group-start# ^long group-len#]
          (dotimes [idx# group-len#]
            (let [~idx-var (+ idx# group-start#)]
              ~@body)))))))


(defn indexed-pmap
  "Legacy function; really just indexed-map-reduce where the default reducer
  is #(apply concat %)."
  ([indexed-pmap-fn num-iters concat-fn]
   (indexed-map-reduce num-iters indexed-pmap-fn concat-fn))
  ([indexed-pmap-fn num-iters]
   (indexed-map-reduce num-iters indexed-pmap-fn #(apply concat %))))


(defn as-iterable ^Iterable [item] item)


(defmacro doiter
  "Execute body for every item in the iterable.  Expecting side effects, returns nil."
  [varname iterable & body]
  `(let [iterable# (as-iterable ~iterable)
         iter# (.iterator iterable#)]
     (loop [continue?# (.hasNext iter#)]
       (when continue?#
         (let [~varname (.next iter#)]
           ~@body
           (recur (.hasNext iter#)))))))
