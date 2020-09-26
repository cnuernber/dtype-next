(ns tech.v3.datatype.io-concat-buffer
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [primitive-math :as pmath])
  (:import [java.util List]
           [tech.v3.datatype PrimitiveIO]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(defmacro ^:private dual-read-macro
  [idx n-elems n-lhs-elems read-method lhs rhs]
  `(do
     (errors/check-idx ~idx ~n-elems)
     (if (< ~idx ~n-lhs-elems)
       (~read-method ~lhs ~idx)
       (~read-method ~rhs (pmath/- ~idx ~n-lhs-elems)))))


(defmacro ^:private dual-write-macro
  [idx n-elems n-lhs-elems write-method lhs rhs value]
  `(do
     (errors/check-idx ~idx ~n-elems)
     (if (< ~idx ~n-lhs-elems)
       (~write-method ~lhs ~idx ~value)
       (~write-method ~rhs (pmath/- ~idx ~n-lhs-elems) ~value))))


(defn- dual-concat-buffer
  ^PrimitiveIO [datatype lhs rhs]
  (let [lhs (dtype-proto/->primitive-io lhs)
        rhs (dtype-proto/->primitive-io rhs)
        n-elems (+ (.lsize lhs) (.lsize rhs))
        lhs-n-elems (.lsize lhs)
        allowsRead (boolean (and (.allowsRead lhs) (.allowsRead rhs)))
        allowsWrite (boolean (and (.allowsWrite lhs) (.allowsWrite rhs)))]
    (reify PrimitiveIO
      (elemwiseDatatype [this] datatype)
      (lsize [this] n-elems)
      (allowsRead [this] allowsRead)
      (allowsWrite [this] allowsWrite)
      (readBoolean [this idx] (dual-read-macro idx n-elems lhs-n-elems .readBoolean lhs rhs))
      (readByte [this idx] (dual-read-macro idx n-elems lhs-n-elems .readByte lhs rhs))
      (readShort [this idx] (dual-read-macro idx n-elems lhs-n-elems .readShort lhs rhs))
      (readChar [this idx] (dual-read-macro idx n-elems lhs-n-elems .readChar lhs rhs))
      (readInt [this idx] (dual-read-macro idx n-elems lhs-n-elems .readInt lhs rhs))
      (readLong [this idx] (dual-read-macro idx n-elems lhs-n-elems .readLong lhs rhs))
      (readFloat [this idx] (dual-read-macro idx n-elems lhs-n-elems .readFloat lhs rhs))
      (readDouble [this idx] (dual-read-macro idx n-elems lhs-n-elems .readDouble lhs rhs))
      (readObject [this idx] (dual-read-macro idx n-elems lhs-n-elems .readObject lhs rhs))
      (writeBoolean [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeBoolean lhs rhs val))
      (writeByte [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeByte lhs rhs val))
      (writeShort [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeShort lhs rhs val))
      (writeChar [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeChar lhs rhs val))
      (writeInt [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeInt lhs rhs val))
      (writeLong [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeLong lhs rhs val))
      (writeFloat [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeFloat lhs rhs val))
      (writeDouble [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeDouble lhs rhs val))
      (writeObject [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeObject lhs rhs val)))))

(defn- as-prim-io ^PrimitiveIO [item] item)

(defmacro ^:private same-len-read-macro
  [idx n-elems buf-len read-method buffers]
  `(do (errors/check-idx ~idx ~n-elems)
       (let [buf-idx# (quot ~idx ~buf-len)
             local-idx# (rem ~idx ~buf-len)]
         (~read-method (as-prim-io (.get ~buffers buf-idx#)) local-idx#))))


(defmacro ^:private same-len-write-macro
  [idx n-elems buf-len write-method buffers val]
  `(do (errors/check-idx ~idx ~n-elems)
       (let [buf-idx# (quot ~idx ~buf-len)
             local-idx# (rem ~idx ~buf-len)]
         (~write-method (as-prim-io (.get ~buffers buf-idx#)) local-idx# ~val))))


(defn- same-len-concat-buffer
  ^PrimitiveIO [datatype buffers]
  (let [counts (mapv dtype-proto/ecount buffers)
        ^List buffers (mapv dtype-proto/->primitive-io buffers)
        _ (assert (apply = counts))
        n-elems (long (apply + counts))
        buf-len (long (first counts))
        allowsRead (boolean (every? #(.allowsRead ^PrimitiveIO %) buffers))
        allowsWrite (boolean (every? #(.allowsWrite ^PrimitiveIO %) buffers))]
    (reify PrimitiveIO
      (elemwiseDatatype [this] datatype)
      (lsize [this] n-elems)
      (allowsRead [this] allowsRead)
      (allowsWrite [this] allowsWrite)
      (readBoolean [this idx] (same-len-read-macro idx n-elems buf-len .readBoolean buffers))
      (readByte [this idx] (same-len-read-macro idx n-elems buf-len .readByte buffers))
      (readShort [this idx] (same-len-read-macro idx n-elems buf-len .readShort buffers))
      (readChar [this idx] (same-len-read-macro idx n-elems buf-len .readChar buffers))
      (readInt [this idx] (same-len-read-macro idx n-elems buf-len .readInt buffers))
      (readLong [this idx] (same-len-read-macro idx n-elems buf-len .readLong buffers))
      (readFloat [this idx] (same-len-read-macro idx n-elems buf-len .readFloat buffers))
      (readDouble [this idx] (same-len-read-macro idx n-elems buf-len .readDouble buffers))
      (readObject [this idx] (same-len-read-macro idx n-elems buf-len .readObject buffers))
      (writeBoolean [this idx val] (same-len-write-macro idx n-elems buf-len .writeBoolean buffers val))
      (writeByte [this idx val] (same-len-write-macro idx n-elems buf-len .writeByte buffers val))
      (writeShort [this idx val] (same-len-write-macro idx n-elems buf-len .writeShort buffers val))
      (writeChar [this idx val] (same-len-write-macro idx n-elems buf-len .writeChar buffers val))
      (writeInt [this idx val] (same-len-write-macro idx n-elems buf-len .writeInt buffers val))
      (writeLong [this idx val] (same-len-write-macro idx n-elems buf-len .writeLong buffers val))
      (writeFloat [this idx val] (same-len-write-macro idx n-elems buf-len .writeFloat buffers val))
      (writeDouble [this idx val] (same-len-write-macro idx n-elems buf-len .writeDouble buffers val))
      (writeObject [this idx val] (same-len-write-macro idx n-elems buf-len .writeObject buffers val)))))


(defmacro ^:private gen-read-macro
  [idx n-elems read-method n-buffers buffers]
  `(do (errors/check-idx ~idx ~n-elems)
       (loop [buf-idx# 0
              idx# ~idx]
         (let [buffer# (as-prim-io (.get ~buffers buf-idx#))
               buf-len# (.lsize buffer#)]
           (if (< idx# buf-len#)
             (~read-method (as-prim-io buffer#) idx#)
             (recur (unchecked-inc buf-idx#) (pmath/- idx# buf-len#)))))))


(defmacro ^:private gen-write-macro
  [idx n-elems write-method n-buffers buffers val]
  `(do (errors/check-idx ~idx ~n-elems)
       (loop [buf-idx# 0
              idx# ~idx]
         (let [buffer# (as-prim-io (.get ~buffers buf-idx#))
               buf-len# (.lsize buffer#)]
           (if (< idx# buf-len#)
             (~write-method (as-prim-io buffer#) idx# ~val)
             (recur (unchecked-inc buf-idx#) (pmath/- idx# buf-len#)))))))



(defn- generalized-concat-buffers
  ^PrimitiveIO [datatype buffers]
  (let [counts (mapv dtype-proto/ecount buffers)
        ^List buffers (mapv dtype-proto/->primitive-io buffers)
        n-elems (long (apply + counts))
        n-buffers (.size buffers)
        allowsRead (boolean (every? #(.allowsRead ^PrimitiveIO %) buffers))
        allowsWrite (boolean (every? #(.allowsWrite ^PrimitiveIO %) buffers))]
    (reify PrimitiveIO
      (elemwiseDatatype [this] datatype)
      (lsize [this] n-elems)
      (allowsRead [this] allowsRead)
      (allowsWrite [this] allowsWrite)
      (readBoolean [this idx] (gen-read-macro idx n-elems .readBoolean n-buffers buffers))
      (readByte [this idx] (gen-read-macro idx n-elems .readByte n-buffers buffers))
      (readShort [this idx] (gen-read-macro idx n-elems .readShort n-buffers buffers))
      (readChar [this idx] (gen-read-macro idx n-elems .readChar n-buffers buffers))
      (readInt [this idx] (gen-read-macro idx n-elems .readInt n-buffers buffers))
      (readLong [this idx] (gen-read-macro idx n-elems .readLong n-buffers buffers))
      (readFloat [this idx] (gen-read-macro idx n-elems .readFloat n-buffers buffers))
      (readDouble [this idx] (gen-read-macro idx n-elems .readDouble n-buffers buffers))
      (readObject [this idx] (gen-read-macro idx n-elems .readObject n-buffers buffers))
      (writeBoolean [this idx val] (gen-write-macro idx n-elems .writeBoolean n-buffers buffers val))
      (writeByte [this idx val] (gen-write-macro idx n-elems .writeByte n-buffers buffers val))
      (writeShort [this idx val] (gen-write-macro idx n-elems .writeShort n-buffers buffers val))
      (writeChar [this idx val] (gen-write-macro idx n-elems .writeChar n-buffers buffers val))
      (writeInt [this idx val] (gen-write-macro idx n-elems .writeInt n-buffers buffers val))
      (writeLong [this idx val] (gen-write-macro idx n-elems .writeLong n-buffers buffers val))
      (writeFloat [this idx val] (gen-write-macro idx n-elems .writeFloat n-buffers buffers val))
      (writeDouble [this idx val] (gen-write-macro idx n-elems .writeDouble n-buffers buffers val))
      (writeObject [this idx val] (gen-write-macro idx n-elems .writeObject n-buffers buffers val)))))



(defn concat-buffers
  "Concatenate a list of buffers into a single unified buffer.  This shares the data and it works when
  the number of buffers is relatively small.  The failure case for this is a large number of small buffers
  where it will be faster to simply copy all the data into a new buffer."
  ([datatype buffers]
   (let [n-buffers (count buffers)]
     (case n-buffers
       0 nil
       1 (first buffers)
       2 (dual-concat-buffer datatype (first buffers) (second buffers))
       (let [buf-lens (map dtype-proto/ecount buffers)]
         (if (apply = buf-lens)
           (same-len-concat-buffer datatype buffers)
           (generalized-concat-buffers datatype buffers))))))
  ([buffers]
   (if (empty? buffers)
     nil
     (let [datatype (reduce casting/widest-datatype
                            (dtype-proto/elemwise-datatype buffers))]
       (concat-buffers datatype buffers)))))
