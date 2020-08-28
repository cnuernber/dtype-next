(ns tech.v3.datatype.io-sub-buffer
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [primitive-math :as pmath])
  (:import [tech.v3.datatype PrimitiveIO ObjectIO LongIO
            DoubleIO BooleanIO]))


(set! *warn-on-reflection* true)


(defn sub-buffer
  (^PrimitiveIO [item ^long offset ^long len]
   (when (< (dtype-base/ecount item)
            (+ offset len))
     (throw (Exception. (format "Item ecount %d is less than offset + len (%d) + (%d)"
                                (dtype-base/ecount item) offset len))))
   (let [item (dtype-base/->io item)
         item-dtype (dtype-base/elemwise-datatype item)
         offset (long offset)
         n-elems (long len)]
     (cond
       (= :boolean item-dtype)
       (reify BooleanIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readBoolean item (pmath/+ idx offset)))
         (write [rdr idx value] (.writeBoolean item (pmath/+ idx offset) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))
       (casting/integer-type? item-dtype)
       (reify LongIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readLong item (pmath/+ idx offset)))
         (write [rdr idx value] (.writeLong item (pmath/+ idx offset) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))
       (casting/float-type? item-dtype)
       (reify DoubleIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readDouble item (pmath/+ idx offset)))
         (write [rdr idx value] (.writeDouble item (pmath/+ idx offset) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item)))
       :else
       (reify ObjectIO
         (elemwiseDatatype [rdr] item-dtype)
         (lsize [rdr] n-elems)
         (read [rdr idx] (.readObject item (pmath/+ idx offset)))
         (write [rdr idx value] (.writeObject item (pmath/+ idx offset) value))
         dtype-proto/PConstantTimeMinMax
         (has-constant-time-min-max? [this]
           (dtype-proto/has-constant-time-min-max? item))
         (constant-time-min [this] (dtype-proto/constant-time-min item))
         (constant-time-max [this] (dtype-proto/constant-time-max item))))))
  (^PrimitiveIO [item ^long offset]
   (sub-buffer item offset (- (dtype-base/ecount item) offset))))
