(ns tech.v3.datatype.dispatch
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.const-reader :refer [const-reader]])
  (:import [tech.v3.datatype ElemwiseDatatype]
           [clojure.lang Seqable]))


(defn typed-map
  [map-fn res-dtype & args]
  (reify
    ElemwiseDatatype
    (elemwiseDatatype [this] res-dtype)
    Iterable
    (iterator [this]
      (.iterator ^Iterable (apply map map-fn args)))
    Seqable
    (seq [this]
      (.seq ^Seqable (apply map map-fn args)))))


(defn typed-map-1
  [scalar-fn res-dtype arg]
  (typed-map scalar-fn res-dtype arg))


(defn typed-map-2
  [scalar-fn res-dtype lhs rhs]
  (typed-map scalar-fn res-dtype lhs rhs))


(defn vectorized-dispatch-1
  "Perform a vectorized dispatch meaning return a different object depending on the
  argument type of arg1.  If arg1 is scalar, use scalar-fn.  If arg1 is an iterable,
  use iterable-fn.  Finally if arg1 is a reader, use reader-fn.  The space the
  operation is to be performed in can be loosely set with :operation-space in the
  options.  The actual space the operation will be performed in (and the return type
  of the system) is a combination of :operation-space if it exists and the elemwise
  datatype of arg1.  See tech.v3.datatype.casting/widest-datatype for more
  information."
  ([scalar-fn iterable-fn reader-fn options arg1]
   (let [arg1-type (arg-type arg1)]
     (if (= arg1-type :scalar)
       (scalar-fn arg1)
       (let [res-dtype (dtype-proto/elemwise-datatype arg1)
             res-dtype (if-let [op-space (:operation-space options)]
                         (casting/widest-datatype res-dtype op-space)
                         res-dtype)]
         (if (= arg1-type :iterable)
           (if iterable-fn
             (iterable-fn res-dtype arg1)
             (typed-map-1 scalar-fn res-dtype arg1))
           (reader-fn res-dtype arg1))))))
  ([scalar-fn iterable-fn reader-fn arg1]
   (vectorized-dispatch-1 scalar-fn iterable-fn reader-fn nil arg1))
  ([scalar-fn reader-fn arg1]
   (vectorized-dispatch-1 scalar-fn nil reader-fn nil arg1)))


;;Minimal versions of ensure functions.
;;full versions are in dtype-base
(defn ensure-iterable
  [item argtype]
  (cond
    (instance? Iterable item)
    item
    (= :scalar argtype)
    ;;not the most efficient but will work.
    (let [item-dtype (dtype-proto/elemwise-datatype item)]
      (reify
        Iterable
        (iterator [it]
          (.iterator ^Iterable (repeat item)))
        ElemwiseDatatype
        (elemwiseDatatype [it] item-dtype)))
    :else
    (dtype-proto/->reader item)))


(defn ensure-reader
  [item n-elems]
  (if (= :scalar (arg-type item))
    (const-reader item n-elems)
    (dtype-proto/->reader item)))


(defn vectorized-dispatch-2
  "Perform a vectorized dispatch meaning return a different object depending on the
  argument types of arg1 and arg2.  If arg1 and arg2 are both scalar, use scalar-fn.
  If neither argument is a reader, use iterable-fn.  Finally if both arguments are
  readers, use reader-fn.  The space the operation is to be performed in can be
  loosely set with :operation-space in the options.  The actual space the operation
  will be performed in (and the return type of the system) is a combination of
  :operation-space if it exists and the elemwise datatypes of arg1 and arg2.  See
  tech.v3.datatype.casting/widest-datatype for more information."
  ([scalar-fn iterable-fn reader-fn options arg1 arg2]
   (let [arg1-type (arg-type arg1)
         arg2-type (arg-type arg2)]
     (if (and (= arg1-type :scalar) (= arg2-type :scalar))
       (scalar-fn arg1 arg2)
       (let [res-dtype (casting/widest-datatype (dtype-proto/elemwise-datatype arg1)
                                                (dtype-proto/elemwise-datatype arg2))
             res-dtype (if-let [op-space (:operation-space options)]
                         (casting/widest-datatype res-dtype op-space)
                         res-dtype)]
         ;;if any of the three arguments are iterable
         (if (or (= arg1-type :iterable) (= arg2-type :iterable))
           (let [arg1 (ensure-iterable arg1 arg1-type)
                 arg2 (ensure-iterable arg2 arg2-type)]
             (if iterable-fn
               (iterable-fn res-dtype arg1 arg2)
               (typed-map-2 scalar-fn res-dtype arg1 arg2)))
           (let [n-elems
                 ;;This is hairy because either the left or right operand may be
                 ;;a constant.
                 (long (cond
                         (and (= :reader arg1-type)
                              (= :reader arg2-type))
                         (let [arg1-ne (dtype-proto/ecount arg1)
                               arg2-ne (dtype-proto/ecount arg2)]
                           (when-not (== arg1-ne arg2-ne)
                             (throw (Exception.
                                     (format "lhs (%d), rhs (%d) n-elems mismatch"
                                             arg1-ne arg2-ne))))
                           arg1-ne)
                         (= :reader arg1-type)
                         (dtype-proto/ecount arg1)
                         :else
                         (dtype-proto/ecount arg2)))
                 arg1 (ensure-reader arg1 n-elems)
                 arg2 (ensure-reader arg2 n-elems)]
             (reader-fn res-dtype arg1 arg2)))))))
  ([scalar-fn reader-fn arg1 arg2]
   (vectorized-dispatch-2 scalar-fn nil reader-fn nil arg1 arg2)))
