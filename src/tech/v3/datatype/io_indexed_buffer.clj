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
     (reify PrimitiveIO
       (elemwiseDatatype [rdr] item-dtype)
       (lsize [rdr] n-elems)
       (readBoolean [this idx] (.readBoolean item (.readLong indexes idx)))
       (readByte [this idx] (.readByte item (.readLong indexes idx)))
       (readShort [this idx] (.readShort item (.readLong indexes idx)))
       (readChar [this idx] (.readChar item (.readLong indexes idx)))
       (readInt [this idx] (.readInt item (.readLong indexes idx)))
       (readLong [this idx] (.readLong item (.readLong indexes idx)))
       (readFloat [this idx] (.readFloat item (.readLong indexes idx)))
       (readDouble [this idx] (.readDouble item (.readLong indexes idx)))
       (readObject [this idx] (.readObject item (.readLong indexes idx)))
       (writeBoolean [this idx val] (.writeBoolean item (.readLong indexes idx) val))
       (writeByte [this idx val] (.writeByte item (.readLong indexes idx) val))
       (writeShort [this idx val] (.writeShort item (.readLong indexes idx) val))
       (writeChar [this idx val] (.writeChar item (.readLong indexes idx) val))
       (writeInt [this idx val] (.writeInt item (.readLong indexes idx) val))
       (writeLong [this idx val] (.writeLong item (.readLong indexes idx) val))
       (writeFloat [this idx val] (.writeFloat item (.readLong indexes idx) val))
       (writeDouble [this idx val] (.writeDouble item (.readLong indexes idx) val))
       (writeObject [this idx val] (.writeObject item (.readLong indexes idx) val))

       (allowsRead [this] (.allowsRead item))
       (allowsWrite [this] (.allowsWrite item))

       dtype-proto/PConstantTimeMinMax
       (has-constant-time-min-max? [this]
         (dtype-proto/has-constant-time-min-max? item))
       (constant-time-min [this] (dtype-proto/constant-time-min item))
       (constant-time-max [this] (dtype-proto/constant-time-max item))))))
