(ns tech.v3.datatype.functional
  (:require [tech.v3.datatype.unary-op :as unary-op]
            [tech.v3.datatype.binary-op :as binary-op]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.binary-pred :as binary-pred]
            [tech.v3.datatype.dispatch :refer [vectorized-dispatch-1
                                               vectorized-dispatch-2]
             :as dispatch]
            [clojure.set :as set]))


(defmacro implement-arithmetic-operations
  []
  (let [binary-ops (set (keys binary-op/builtin-ops))
        unary-ops (set (keys unary-op/builtin-ops))
        dual-ops (set/intersection binary-ops unary-ops)
        unary-ops (set/difference unary-ops dual-ops)]
    (println binary-ops unary-ops dual-ops unary-ops)
    `(do
       ~@(->>
          unary-ops
          (map
           (fn [opname]
             (let [op (unary-op/builtin-ops opname)
                   op-meta (meta op)
                   op-sym (vary-meta (symbol (name opname))
                                     merge op-meta)]
               `(defn ~(with-meta op-sym
                         {:unary-operator opname})
                  ([~'x ~'options]
                   ~'x)
                  ([~'x]
                   ~'x))))))
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
                 `(defn ~(with-meta op-sym
                           {:binary-operator opname})
                    ([~'x]
                     ~'x)
                    ([~'x ~'y])
                    [~'x ~'y]
                    ([~'x ~'y & ~'args]
                     [~'x ~'y ~'args]))
                 `(defn ~(with-meta op-sym
                           {:binary-operator opname})
                    ([~'x ~'y]
                     [~'x ~'y])
                    ([~'x ~'y & ~'args]
                     [~'x ~'y ~'args]))))))))))
