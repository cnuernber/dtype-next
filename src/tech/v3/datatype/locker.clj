(ns tech.v3.datatype.locker
  "Threadsafe access to a key-value map.  Unlike atoms and swap! your
  update/initialization functions are never ran in parallel which helps if, for instance,
  initialization is a long running, memory intensive operation there will only be one
  running even if multiple threads request a value simultaneously.

```clojure
user> (require '[tech.v3.datatype.locker :as locker])
nil
user> (locker/update! ::latest (fn [k v]
                                 (println (format \"k is %s v is %s\" k v))
                                 ::updated))
k is :user/latest v is null
:user/updated
user> (locker/update! ::latest (fn [k v]
                                 (println (format \"k is %s v is %s\" k v))
                                 ::updated))
k is :user/latest v is :user/updated
:user/updated
```"
  (:require [tech.v3.datatype.jvm-map :as jvm-map])
  (:import [java.util.concurrent ConcurrentHashMap]))


(set! *warn-on-reflection* true)


(defonce ^{:tag ConcurrentHashMap} locker* (delay (ConcurrentHashMap.)))
(defn locker ^ConcurrentHashMap [] @locker*)


(defn remove!
  "Remove a key - returns the value if any that existed in that location."
  [key]
  (.remove (locker) key))


(defn compute-if-absent!
  "Compute a new value if non exists for this key.  Key cannot be nil.
  init-fn will only ever be called once and is passed the key as its first
  argument."
  [key init-fn]
  (jvm-map/compute-if-absent! (locker) key init-fn))


(defn compute-if-present!
  "Compute a new value if one already exists for this key.  Key cannot be nil.
  update-fn is passed the key and the preexisting value."
  [key update-fn]
  (jvm-map/compute-if-present! (locker) key update-fn))


(defn exists?
  [key]
  (.containsKey (locker) key))


(defn update!
  "Update a value.  update-fn gets passed the key and the value already present (if any)."
  [key update-fn]
  (jvm-map/compute! (locker) key update-fn))


(defn value
  ([key missing-value]
   (.getOrDefault (locker) key missing-value))
  ([key] (value key nil)))
