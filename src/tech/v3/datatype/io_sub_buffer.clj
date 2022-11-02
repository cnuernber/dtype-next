(ns tech.v3.datatype.io-sub-buffer
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [tech.v3.datatype Buffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn sub-buffer
  (^Buffer [item ^long offset ^long len]
   (when (< (dtype-proto/ecount item)
            (+ offset len))
     (throw (Exception. (format "Item ecount %d is less than offset + len (%d) + (%d)"
                                (dtype-proto/ecount item) offset len))))
   (let [^Buffer item (dtype-proto/->buffer item)
         item-dtype (dtype-proto/elemwise-datatype item)
         offset (long offset)
         n-elems (long len)]
     (reify Buffer
       (elemwiseDatatype [rdr] item-dtype)
       (lsize [rdr] n-elems)
       (readLong [this idx] (.readLong item (pmath/+ offset idx)))
       (readDouble [this idx] (.readDouble item (pmath/+ offset idx)))
       (readObject [this idx] (.readObject item (pmath/+ offset idx)))
       (writeLong [this idx val] (.writeLong item (pmath/+ offset idx) val))
       (writeDouble [this idx val] (.writeDouble item (pmath/+ offset idx) val))
       (writeObject [this idx val] (.writeObject item (pmath/+ offset idx) val))

       (allowsRead [this] (.allowsRead item))
       (allowsWrite [this] (.allowsWrite item))

       dtype-proto/PConstantTimeMinMax
       (has-constant-time-min-max? [this]
         (dtype-proto/has-constant-time-min-max? item))
       (constant-time-min [this] (dtype-proto/constant-time-min item))
       (constant-time-max [this] (dtype-proto/constant-time-max item)))))
  (^Buffer [item ^long offset]
   (sub-buffer item offset (- (dtype-proto/ecount item) offset))))
