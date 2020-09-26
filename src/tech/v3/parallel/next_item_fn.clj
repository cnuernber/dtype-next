(ns tech.v3.parallel.next-item-fn
  (:require [tech.v3.parallel.for :as pfor]))


(defn create-next-item-fn
  "Given a sequence return a function that each time called (with no arguments)
  returns the next item in the sequence, iterable, or stream in a threadsafe but
  mutable fashion."
  [item-sequence]
  (let [iterator (pfor/->iterator item-sequence)]
    (fn []
      (locking iterator
        (when (.hasNext iterator)
          (.next iterator))))))
