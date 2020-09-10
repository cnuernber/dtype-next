(ns tech.v3.datatype.packing
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch])
  (:import  [java.util.concurrent ConcurrentHashMap]
            [tech.v3.datatype ObjectReader LongReader PrimitiveIO]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defonce ^ConcurrentHashMap pack-table (ConcurrentHashMap.))
(defonce ^ConcurrentHashMap unpack-table (ConcurrentHashMap.))

(defn add-packed-datatype!
  [object-cls object-datatype
   packed-datatype primitive-datatype
   pack-fn unpack-fn]
  (casting/add-object-datatype! object-datatype object-cls)
  (casting/alias-datatype! packed-datatype primitive-datatype)
  (let [pack-entry {:object-datatype object-datatype
                    :packed-datatype packed-datatype
                    :primitive-datatype primitive-datatype
                    :pack-fn pack-fn
                    :unpack-fn unpack-fn}]
    (.put pack-table object-datatype pack-entry)
    (.put unpack-table packed-datatype pack-entry)))


(defn packed-datatype?
  [datatype]
  (.contains unpack-table datatype))

(defn unpacked-datatype?
  [datatype]
  (.contains pack-table datatype))


(defn unpack
  [item]
  (if-let [{:keys [unpack-fn object-datatype primitive-datatype]}
           (.get unpack-table (dtype-proto/elemwise-datatype item))]
    (dispatch/vectorized-dispatch-1
     item unpack-fn
     #(fn [item _op-datatype]
        (let [^PrimitiveIO item (dtype-proto/->reader item)]
          (if (casting/integer-type? primitive-datatype)
            (reify ObjectReader
              (elemwiseDatatype [rdr] object-datatype)
              (lsize [rdr] (.lsize item))
              (readObject [rdr idx]
                (unpack-fn (.readLong item idx))))
            (reify ObjectReader
              (elemwiseDatatype [rdr] object-datatype)
              (lsize [rdr] (.lsize item))
              (readObject [rdr idx]
                (unpack-fn (.readObject item idx))))))))
    item))


(defn pack
  [item]
  (if-let [{:keys [pack-fn packed-datatype primitive-datatype]}
           (.get pack-table (dtype-proto/elemwise-datatype item))]
    (dispatch/vectorized-dispatch-1
     item pack-fn
     #(fn [item _op-datatype]
        (let [^PrimitiveIO item (dtype-proto/->reader item)]
          (if (casting/integer-type? primitive-datatype)
            (reify LongReader
              (elemwiseDatatype [rdr] packed-datatype)
              (lsize [rdr] (.lsize item))
              (readLong [rdr idx]
                (unchecked-long (pack-fn (.readObject item idx)))))
            (reify ObjectReader
              (elemwiseDatatype [rdr] packed-datatype)
              (lsize [rdr] (.lsize item))
              (readObject [rdr idx]
                (pack-fn (.readObject item idx))))))))
    item))
