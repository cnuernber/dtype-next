(ns tech.v3.datatype.jvm-map
  "Creation and higher-level  manipulation for `java.util.HashMap` and
  `java.util.concurrent.ConcurrentHashMap`."
  (:require [tech.v3.datatype.protocols :as dt-proto])
  (:import [java.util Map HashMap Map$Entry Set HashSet]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiConsumer BiFunction Function])
  (:refer-clojure :exclude [hash-map get set]))


(set! *warn-on-reflection* true)


(defn hash-map
  (^HashMap []
   (HashMap.))
  (^HashMap [init-size]
   (HashMap. (int init-size))))


(defn set
  ^Set [] (HashSet. ))


(defn concurrent-hash-map
  (^ConcurrentHashMap []
   (ConcurrentHashMap.))
  (^ConcurrentHashMap [init-size]
   (ConcurrentHashMap. (int init-size))))


(defn concurrent-set
  (^Set [] (.keySet (concurrent-hash-map))))


(defn put!
  "Put a value into a map"
  [^Map map k v]
  (.put map k v)
  map)


(defn add!
  "Add a value to a set"
  [^Set set k]
  (.add set k)
  set)


(defn entry-key
  "Given a map$Entry, return the key"
  [^Map$Entry v]
  (when v (.getKey v)))


(defn entry-value
  "Given a map$Entry, return the key"
  [^Map$Entry v]
  (when v (.getValue v)))


(defn get
  "Get a value from a map with a potential default.  Missing values will be nil otherwise."
  ([^Map map k]
   (.get map k))
  ([^Map map k default-value]
   (.getOrDefault map k default-value)))


(defmacro bi-consumer
  "Create a java.util.function.BiConsumer.  karg will be the name of the first argument
  to the consumer, varg will be the name of the second.  Code will be output inline into
  the consumer's accept function."
  [karg varg code]
  `(reify BiConsumer
     (accept [this# ~karg ~varg]
       ~code)))


(defn ->bi-consumer
  "Take a thing and return a bi-consumer.  If already a bi-consumer return as is,
    else calls `(bi-consumer x y (ifn x y))`"
  ^BiConsumer [ifn]
  (if (instance? BiConsumer ifn)
    ifn
    (bi-consumer x y (ifn x y))))


(defmacro bi-function
  "Create a java.util.function.BiFunction.  karg will be the name of the first argument,
  varg will be the name of the second argument.  code will be output inline into the
  function's apply function."
  [karg varg code]
  `(reify BiFunction
     (apply [this# ~karg ~varg]
       ~code)))


(defn ->bi-function
  "Take a thing and return a bi-function.  If already a bi-function return as is,
  else calls `(bi-function x y (ifn x y))`"
  ^BiFunction [ifn]
  (if (instance? BiFunction ifn)
    ifn
    (bi-function x y (ifn x y))))


(defmacro function
  "Create a java.util.function.Function.  karg will be the name of the argument,
   will be output inline into the function's apply method."
  [karg code]
  `(reify Function
     (apply [this# ~karg]
       ~code)))


(defn ->function
  "Convert an ifn to a java.util.function.Function.  If ifn is already a Function,
  return with no changes.  Else calls `(function x (ifn x))`"
  ^Function [ifn]
  (if (instance? Function ifn)
    ifn
    (function x (ifn x))))


(defn concurrent-hash-map?
  "Return true if this is a concurrent hash map."
  [item]
  (instance? ConcurrentHashMap item))


(defn foreach!
  "Perform an operation for each entry in a map.  Returns the op.  This method will by
  default be serial unless parallel? is true in which case if the item in question
  is a concurrent hash map then the parallel version of foreach is used."
  ([item op & [parallel?]]
   (let [consumer (->bi-consumer op)]
     (if (and parallel? (concurrent-hash-map? item))
       (.forEach ^ConcurrentHashMap item 1 consumer)
       (.forEach ^Map item consumer)))
   op))


(defn merge-with!
  "Merge `rhs` into `lhs` using op to resolve conflicts.  Op gets passed two arguments,
  `lhs-val` `rhs-val`.  If both rhs and lhs are concurrent hash maps then the merge
  is performed in parallel.

  Returns `lhs`."
  [lhs rhs op]
  (let [merge-fn (->bi-function op)
        foreach-consumer (bi-consumer k v (.merge ^Map lhs k v merge-fn))]
    (foreach! rhs foreach-consumer (and (concurrent-hash-map? lhs)
                                        (concurrent-hash-map? rhs))))
  lhs)


(defn compute-fn!
  "Given a map and a function to call if the value is in the map, return a function that
  takes a k and returns the result of calling `(.compute map k (->bi-function val-fn))`.
  `val-fn` gets passed the key and existing value (if any)."
  [map val-fn]
  (let [val-fn (->bi-function val-fn)]
    #(.compute ^Map map % val-fn)))


(defn compute-if-absent-fn!
    "Given a map and a function to call if the value is not in the map, return a function that
  takes a k and returns the result of calling `(.compute map k (->function val-fn))`.
  `val-fn` gets passed the key when the value is not in the map."
  [map val-fn]
  (let [val-fn (->function val-fn)]
    #(.computeIfAbsent ^Map map % val-fn)))


(defn compute-if-present-fn!
    "Given a map and a function to call if the value is in the map, return a function that
  takes a k and returns the result of calling
  `(.computeIfPresent map k (->bi-function val-fn))`.
  `val-fn` gets passed the existing value and new value is in the map."
  [map val-fn]
  (let [val-fn (->bi-function val-fn)]
    #(.computeIfPresent ^Map map % val-fn)))


(extend-protocol dt-proto/PClone
  HashMap
  (clone [item] (.clone item))
  ConcurrentHashMap
  (clone [item] (ConcurrentHashMap. item)))
