(ns tech.v3.datatype.ordered-consumer
  "Consumer that keeps track of input so it can detect if, for instance, a range
  can represent the result"
  (:require [tech.v3.datatype.argops :as argops])
  (:import [java.util.function DoubleConsumer LongConsumer Consumer]
           [java.util Comparator]
           [clojure.lang IDeref]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro ^:private inline-compare
  ([a b comp-form]
   `(case (~comp-form ~a ~b)
      1 :tech.numerics/>
      0 :tech.numerics/==
      -1 :tech.numerics/<))
  ([a b comparator _comp-marker?]
   `(case (.compare ~comparator ~a ~b)
     1 :tech.numerics/>
     0 :tech.numerics/==
     -1 :tech.numerics/<)))



(deftype LongOrderedConsumer [^{:unsynchronized-mutable true} order
                              ^{:unsynchronized-mutable true
                                :tag long} n-elems
                              ^{:unsynchronized-mutable true
                                :tag long} first-value
                              ^{:unsynchronized-mutable true
                                :tag long} last-value]
  LongConsumer
  (accept [_this val]
    (set! n-elems (unchecked-inc n-elems))
    (case n-elems
      1 (set! first-value val)
      2 (set! order (inline-compare val last-value Long/compare))
      (let [new-order (inline-compare val last-value Long/compare)]
        (when-not (identical? new-order :tech.numerics/==)
          (if (identical? order :tech.numerics/==)
            (set! order new-order)
            (when-not (identical? new-order order)
              (set! order :tech.numerics/unordered))))))
    (set! last-value val))
  IDeref
  (deref [_this] {:order order
           `      :last-value last-value}))


(defn long-ordered-consumer
  (^LongConsumer [_options]
   (LongOrderedConsumer. :tech.numerics/== 0 Long/MIN_VALUE Long/MIN_VALUE))
  (^LongConsumer [] (long-ordered-consumer nil)))


(deftype DoubleOrderedConsumer [^{:unsynchronized-mutable true} order
                              ^{:unsynchronized-mutable true
                                :tag double} n-elems
                              ^{:unsynchronized-mutable true
                                :tag double} first-value
                              ^{:unsynchronized-mutable true
                                :tag double} last-value]
  DoubleConsumer
  (accept [_this val]
    (set! n-elems (unchecked-inc n-elems))
    (case n-elems
      1 (set! first-value val)
      2 (set! order (inline-compare val last-value Double/compare))
      (let [new-order (inline-compare val last-value Double/compare)]
        (when-not (identical? new-order :tech.numerics/==)
          (if (identical? order :tech.numerics/==)
            (set! order new-order)
            (when-not (identical? new-order order)
              (set! order :tech.numerics/unordered))))))
    (set! last-value val))
  IDeref
  (deref [_this] {:order order
                  :last-value last-value}))


(defn double-ordered-consumer
  ^DoubleConsumer []
  (DoubleOrderedConsumer. :tech.numerics/== 0 Double/NaN Double/NaN))


(deftype OrderedConsumer [^{:unsynchronized-mutable true} order
                          ^{:unsynchronized-mutable true
                            :tag long} n-elems
                          ^{:unsynchronized-mutable true} first-value
                          ^{:unsynchronized-mutable true} last-value
                          ^Comparator comparator]
  Consumer
  (accept [_this val]
    (set! n-elems (unchecked-inc n-elems))
    (case n-elems
      1 (set! first-value val)
      2 (set! order (inline-compare val last-value comparator true))
      (let [new-order (inline-compare val last-value comparator true)]
        (when-not (identical? new-order :tech.numerics/==)
          (if (identical? order :tech.numerics/==)
            (set! order new-order)
            (when-not (identical? new-order order)
              (set! order :tech.numerics/unordered))))))
    (set! last-value val))
  IDeref
  (deref [_this] {:order order
                  :last-value last-value}))


(defn ordered-consumer
  (^Consumer [datatype options]
   (let [comparator (-> (argops/find-base-comparator (:comparator options)
                                                     datatype)
                        (argops/->comparator))]
     (OrderedConsumer. :tech.numercs/== 0 nil nil comparator)))
  (^Consumer [datatype]
   (ordered-consumer datatype nil))
  (^Consumer []
   (ordered-consumer :float64)))
