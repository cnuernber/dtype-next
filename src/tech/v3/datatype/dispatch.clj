(ns tech.v3.datatype.dispatch
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.const-reader :refer [const-reader]]))


(defn vectorized-dispatch-1
  ([arg1 scalar-fn iterable-fn reader-fn options]
   (let [arg1-type (arg-type arg1)]
     (case arg1-type
       :scalar
       (scalar-fn arg1)
       :iterable
       (if iterable-fn
         (iterable-fn arg1)
         (map scalar-fn arg1))
       (let [res-dtype (dtype-base/elemwise-datatype arg1)
             res-dtype (if-let [op-space (:operation-space options)]
                         (casting/widest-datatype res-dtype op-space)
                         res-dtype)]
         (reader-fn arg1 res-dtype)))))
  ([arg1 scalar-fn iterable-fn reader-fn]
   (vectorized-dispatch-1 arg1 scalar-fn iterable-fn reader-fn nil))
  ([arg1 scalar-fn reader-fn]
   (vectorized-dispatch-1 arg1 scalar-fn nil reader-fn nil)))


(defn ensure-iterable
  [item argtype]
  (cond
    (instance? Iterable item)
    item
    (= :scalar argtype)
    ;;not the most efficient but will work.
    (repeat item)
    :else
    (if-let [rdr (dtype-base/->reader item)]
      rdr
      (throw (Exception. (format "Item %s is not convertible to iterable" item))))))


(defn ensure-reader
  [item n-elems]
  (if (= :scalar (arg-type item))
    (const-reader item n-elems)
    (dtype-base/->reader item)))


(defn vectorized-dispatch-2
  ([arg1 arg2 scalar-fn reader-fn options]
   (let [arg1-type (arg-type arg1)
         arg2-type (arg-type arg2)]
     (cond
       (clojure.core/and (= arg1-type :scalar) (= arg2-type :scalar))
       (scalar-fn arg1 arg2)
       ;;if any of the three arguments are iterable
       (clojure.core/or (= arg1-type :iterable)
                        (= arg2-type :iterable))
       (let [arg1 (ensure-iterable arg1 arg1-type)
             arg2 (ensure-iterable arg2 arg2-type)]
         (map scalar-fn arg1 arg2))
       :else
       (let [n-elems (long (cond
                             (clojure.core/and (= :reader arg1-type)
                                               (= :reader arg2-type))
                             (let [arg1-ne (dtype-base/ecount arg1)
                                   arg2-ne (dtype-base/ecount arg2)]
                               (when-not (== arg1-ne arg2-ne)
                                 (throw (Exception.
                                         (format "lhs (%d), rhs (%d) n-elems mismatch"
                                                 arg1-ne arg2-ne))))
                               arg1-ne)
                             (= :reader arg1-type)
                             (dtype-base/ecount arg1)
                             :else
                             (dtype-base/ecount arg2)))
             arg1 (ensure-reader arg1 n-elems)
             arg2 (ensure-reader arg2 n-elems)
             res-dtype (casting/widest-datatype (dtype-base/elemwise-datatype arg1)
                                                (dtype-base/elemwise-datatype arg2))
             res-dtype (if-let [op-space (:operation-space options)]
                         (casting/widest-datatype res-dtype op-space)
                         res-dtype)]
         (reader-fn arg1 arg2 res-dtype)))))
  ([arg1 arg2 scalar-fn reader-fn]
   (vectorized-dispatch-2 arg1 arg2 scalar-fn reader-fn)))
