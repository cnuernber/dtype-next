(ns tech.v3.datatype.packing
  "Implements the 'packed/unpacked' concept in datatype.  This allows us to take objects
  like LocalDate's and store them in int32 storage which dramatically decreases their
  size."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.dispatch :as dispatch])
  (:import  [java.util.concurrent ConcurrentHashMap]
            [tech.v3.datatype ObjectReader LongReader Buffer]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defonce ^{:private true
           :tag ConcurrentHashMap} pack-table (ConcurrentHashMap.))
(defonce ^{:private true
           :tag ConcurrentHashMap} unpack-table (ConcurrentHashMap.))

(defn add-packed-datatype!
  "Add a datatype that you wish to be packed into a single scalar numeric value."
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
  "Returns true of this is a datatype that could be unpacked."
  [datatype]
  (.containsKey unpack-table datatype))

(defn unpack-datatype
  "Returns the unpacked datatype for this packed datatype."
  [datatype]
  (get-in unpack-table [datatype :object-datatype] datatype))

(defn unpacked-datatype?
  "Returns true if this is a datatype that could be packed."
  [datatype]
  (.containsKey pack-table datatype))

(defn pack-datatype
  "Returns the packed datatype for this unpacked datatype."
  [datatype]
  (get-in pack-table [datatype :packed-datatype] datatype))

(defn unpack
  "Unpack a scalar, iterable, or a reader.  If the item is not a packed datatype
  this is a no-op."
  [item]
  (if-let [{:keys [unpack-fn object-datatype primitive-datatype]}
           (.get unpack-table (dtype-proto/elemwise-datatype item))]
    (dispatch/vectorized-dispatch-1
     unpack-fn
     nil
     (fn [_op-datatype item]
       (let [^Buffer item (dtype-proto/->reader item)]
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
               (unpack-fn (.readObject item idx)))))))
     nil
     item)
    item))


(defn pack
  "Pack a scalar, iterable, or a reader into a new scalar, iterable or a reader.
  If this isn't a packable datatype this is a no-op."
  [item]
  (if-let [{:keys [pack-fn packed-datatype primitive-datatype]}
           (.get pack-table (dtype-proto/elemwise-datatype item))]
    (dispatch/vectorized-dispatch-1
     pack-fn
     nil
     (fn [_op-datatype item]
       (let [^Buffer item (dtype-proto/->reader item)]
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
               (pack-fn (.readObject item idx)))))))
     item)
    item))

(defn wrap-with-packing
  [datatype src-fn]
  (if-let [{:keys [pack-fn]}
           (.get pack-table datatype)]
    #(pack-fn (src-fn %))
    src-fn))
