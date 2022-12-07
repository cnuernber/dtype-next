(ns tech.v3.datatype.io-indexed-buffer
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [ham-fisted.api :as hamf])
  (:import [tech.v3.datatype Buffer]
           [clojure.lang IFn$OLO IFn$ODO]))


(set! *warn-on-reflection* true)


(defn indexed-buffer
  "Create a new Buffer implementation that indexes into a previous
  Buffer implementation via the provided indexes."
  (^Buffer [indexes item]
   ;;Check if this is much more efficiently addressed as a sub-buffer operation.
   (if (and (dtype-proto/convertible-to-range? indexes)
            (== 1 (long (-> (dtype-proto/->range indexes nil)
                            (dtype-proto/range-increment)))))
     (let [r (dtype-proto/->range indexes nil)
           s (long (dtype-proto/range-start r))
           ne (long (dtype-proto/ecount indexes))]
       (dtype-proto/sub-buffer item s ne))
     (let [indexes (dtype-base/->reader indexes)
           item (dtype-base/->buffer item)
           item-dtype (dtype-base/elemwise-datatype item)
           n-elems (.lsize indexes)]
       (reify Buffer
         (elemwiseDatatype [rdr] item-dtype)
         (subBuffer [rdr sidx eidx] (indexed-buffer (.subBuffer indexes sidx eidx) item))
         (lsize [rdr] n-elems)
         (readLong [this idx] (.readLong item (.readLong indexes idx)))
         (readDouble [this idx] (.readDouble item (.readLong indexes idx)))
         (readObject [this idx] (.readObject item (.readLong indexes idx)))
         (writeLong [this idx val] (.writeLong item (.readLong indexes idx) val))
         (writeDouble [this idx val] (.writeDouble item (.readLong indexes idx) val))
         (writeObject [this idx val] (.writeObject item (.readLong indexes idx) val))

         (allowsRead [this] (.allowsRead item))
         (allowsWrite [this] (.allowsWrite item))
         ;;Used in high performance copies
         (fillRange [this sidx src]
           (case (casting/simple-operation-space (dtype-base/elemwise-datatype item))
             :int64
             (reduce (hamf/indexed-long-accum
                      acc idx v
                      (.writeLong ^Buffer acc (.readLong indexes (+ sidx idx)) v) acc)
                     item
                     src)
             :float64
             (reduce (hamf/indexed-double-accum
                      acc idx v
                      (.writeDouble ^Buffer acc (.readLong indexes (+ sidx idx)) v) acc)
                     item
                     src)
             (reduce (hamf/indexed-accum
                      acc idx v
                      (.writeObject ^Buffer acc (.readObject indexes (+ sidx idx)) v) acc)
                     item
                     src)))
         (reduce [this rfn init]
           (.reduce indexes
                    (cond
                      (instance? IFn$OLO rfn)
                      (hamf/long-accumulator
                       acc v
                       (.invokePrim ^IFn$OLO rfn acc (.readLong item v)))
                      (instance? IFn$ODO rfn)
                      (hamf/long-accumulator
                       acc v
                       (.invokePrim ^IFn$ODO rfn acc (.readDouble item v)))
                      :else
                      (hamf/long-accumulator
                       acc v
                       (rfn acc (.readObject item v))))
                    init))
         (parallelReduction [this init-val-fn rfn merge-fn options]
           (.parallelReduction indexes init-val-fn
                               (cond
                                 (instance? IFn$OLO rfn)
                                 (hamf/long-accumulator
                                  acc v (.invokePrim ^IFn$OLO rfn acc (.readLong item v)))
                                 (instance? IFn$ODO rfn)
                                 (hamf/long-accumulator
                                  acc v (.invokePrim ^IFn$ODO rfn acc (.readDouble item v)))
                                 :else
                                 (hamf/long-accumulator
                                  acc v (rfn acc (.readObject item v))))
                               merge-fn
                               options))
         dtype-proto/PElemwiseReaderCast
         (elemwise-reader-cast [this new-dtype]
           (indexed-buffer indexes (dtype-proto/elemwise-reader-cast item new-dtype)))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))))))
