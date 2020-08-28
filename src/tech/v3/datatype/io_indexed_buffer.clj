(ns tech.v3.datatype.io-indexed-buffer
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting])
  (:import [tech.v3.datatype PrimitiveIO ObjectIO LongIO
            DoubleIO BooleanIO]))


(set! *warn-on-reflection* true)


(defn indexed-buffer
  (^PrimitiveIO [indexes item]
   (let [indexes (dtype-base/->reader indexes)
         item (dtype-base/->io item)
         item-dtype (dtype-base/elemwise-datatype item)
         n-elems (.lsize indexes)]
     (cond
       (= :boolean item-dtype)
       (reify BooleanIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readBoolean item (.readLong indexes idx)))
         (write [rdr idx value] (.writeBoolean item (.readLong indexes idx) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))
       (casting/integer-type? item-dtype)
       (reify LongIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readLong item (.readLong indexes idx)))
         (write [rdr idx value] (.writeLong item (.readLong indexes idx) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))
       (casting/float-type? item-dtype)
       (reify DoubleIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readDouble item (.readLong indexes idx)))
         (write [rdr idx value] (.writeDouble item (.readLong indexes idx) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))
       :else
       (reify ObjectIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readObject item (.readLong item idx)))
         (write [rdr idx value] (.writeObject item (.readLong item idx) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))))))
