(ns tech.v3.datatype.functional
  (:require [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.readers :as readers]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.reductions :as dtype-reductions]
            [primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype BinaryOperator PrimitiveReader
            LongReader DoubleReader ObjectReader])
  (:refer-clojure :exclude [+ - / *
                            <= < >= >
                            identity
                            min max
                            bit-xor bit-and bit-and-not bit-not bit-set bit-test
                            bit-or bit-flip bit-clear
                            bit-shift-left bit-shift-right unsigned-bit-shift-right
                            quot rem cast not and or]))


(defn vectorized-dispatch-1
  ([arg1 scalar-fn reader-fn options]
   (let [arg1-type (arg-type arg1)]
     (case arg1-type
       :scalar
       (scalar-fn arg1)
       :iterable
       (map scalar-fn arg1)
       (let [res-dtype (dtype-base/elemwise-datatype arg1)
             res-dtype (if-let [op-space (:operation-space options)]
                         (casting/widest-datatype res-dtype op-space)
                         res-dtype)]
         (reader-fn arg1 res-dtype)))))
  ([arg1 scalar-fn reader-fn]
   (vectorized-dispatch-1 arg1 scalar-fn reader-fn)))


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
    (readers/const-reader item n-elems)
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


(defmacro implement-arithmetic-operations
  []
  (let [binary-ops (set (keys binary-op/builtin-ops))
        unary-ops (set (keys unary-op/builtin-ops))
        dual-ops (set/intersection binary-ops unary-ops)
        unary-ops (set/difference unary-ops dual-ops)]
    `(do
       ~@(->>
          unary-ops
          (map
           (fn [opname]
             (let [op (unary-op/builtin-ops opname)
                   op-meta (meta op)
                   op-sym (vary-meta (symbol (name opname))
                                     merge op-meta)]
               `(defn ~op-sym
                  [~'x]
                  (vectorized-dispatch-1
                   ~'x
                   (unary-op/builtin-ops ~opname)
                   #(unary-op/primitive-reader
                     %1
                     (unary-op/builtin-ops ~opname)
                     %2)
                   ~op-meta))))))
       ~@(->>
          binary-ops
          (map
           (fn [opname]
             (let [op (binary-op/builtin-ops opname)
                   op-meta (meta op)
                   op-sym (vary-meta (symbol (name opname))
                                     merge op-meta)
                   dual-op? (dual-ops opname)]
               (if dual-op?
                 `(defn ~op-sym
                    ([~'x]
                     (vectorized-dispatch-1
                      ~'x
                      (unary-op/builtin-ops ~opname)
                      #(unary-op/primitive-reader
                        %1
                        (unary-op/builtin-ops ~opname)
                        %2)
                      ~op-meta))
                    ([~'x ~'y]
                     (vectorized-dispatch-2
                      ~'x ~'y
                      (binary-op/builtin-ops ~opname)
                      #(binary-op/primitive-reader
                        %1 %2
                        (binary-op/builtin-ops ~opname)
                        %3)
                      ~op-meta)))
                 `(defn ~op-sym
                    [~'x ~'y]
                    (vectorized-dispatch-2
                     ~'x ~'y
                     (binary-op/builtin-ops ~opname)
                     #(binary-op/primitive-reader
                       %1 %2
                       (binary-op/builtin-ops ~opname)
                       %3)
                     ~op-meta))))))))))


(implement-arithmetic-operations)


(defn round
  "Returns a long reader but operates in double space."
  [arg]
  (vectorized-dispatch-1
   arg #(Math/round (double %))
   (fn [rdr op-dtype]
     (let [src-rdr (dtype-base/->reader rdr)]
       (reify LongReader
         (lsize [rdr] (.lsize src-rdr))
         (read [rdr idx]
           (Math/round (.readDouble src-rdr idx))))))
   nil))

;;Implement only reductions that we know we will use.
(defn reduce-+
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   rdr (:+ binary-op/builtin-ops)))


(defn sum [rdr] (reduce-+ rdr))


(defn reduce-*
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   rdr (:* binary-op/builtin-ops)))


(defn reduce-max
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   rdr (:max binary-op/builtin-ops)))


(defn reduce-min
  [rdr]
  (dtype-reductions/commutative-binary-reduce
   rdr (:min binary-op/builtin-ops)))


(defn mean
  [rdr]
  (pmath// (double (reduce-+ rdr))
           (double (dtype-base/ecount rdr))))
