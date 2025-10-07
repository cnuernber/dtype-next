(ns tech.v3.datatype.update-reader
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.hamf-proto :as hamf-proto]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.bitmap :as bitmap])
  (:import [tech.v3.datatype ObjectReader Buffer]
           [org.roaringbitmap RoaringBitmap]
           [java.util Map]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn update-reader
  "Create a new reader that uses values from the update-map else uses values
  from the src-reader if the update map values do not exist."
  [src-reader update-map]
  (let [dtype (hamf-proto/elemwise-datatype src-reader)
        ^Buffer src-reader (dtype-proto/->buffer src-reader)
        n-elems (.lsize src-reader)
        ^Map update-map (typecast/->java-map update-map)
        ^RoaringBitmap bitmap (if (dtype-proto/convertible-to-bitmap? update-map)
                                (dtype-proto/as-roaring-bitmap update-map)
                                (bitmap/->bitmap (keys update-map)))]
    (reify ObjectReader
      (elemwiseDatatype [rdr] dtype)
      (lsize [rdr] n-elems)
      (readObject [rdr idx]
        (if (.contains bitmap (unchecked-int idx))
          (.get update-map idx)
          (.readObject src-reader idx))))))
