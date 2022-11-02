(ns tech.v3.datatype.io-concat-buffer
  (:require [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [com.github.ztellman.primitive-math :as pmath])
  (:import [java.util List]
           [tech.v3.datatype Buffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare concat-buffers)

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
  ^Buffer [datatype lhs rhs]
  (let [lhs (dtype-proto/->buffer lhs)
        rhs (dtype-proto/->buffer rhs)
        n-elems (+ (.lsize lhs) (.lsize rhs))
        lhs-n-elems (.lsize lhs)
        allowsRead (boolean (and (.allowsRead lhs) (.allowsRead rhs)))
        allowsWrite (boolean (and (.allowsWrite lhs) (.allowsWrite rhs)))]
    (reify Buffer
      (elemwiseDatatype [this] datatype)
      (lsize [this] n-elems)
      (allowsRead [this] allowsRead)
      (allowsWrite [this] allowsWrite)
      (readLong [this idx] (dual-read-macro idx n-elems lhs-n-elems .readLong lhs rhs))
      (readDouble [this idx] (dual-read-macro idx n-elems lhs-n-elems .readDouble lhs rhs))
      (readObject [this idx] (dual-read-macro idx n-elems lhs-n-elems .readObject lhs rhs))
      (writeLong [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeLong lhs rhs val))
      (writeDouble [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeDouble lhs rhs val))
      (writeObject [this idx val] (dual-write-macro idx n-elems lhs-n-elems .writeObject lhs rhs val))
      dtype-proto/PElemwiseReaderCast
      (elemwise-reader-cast [this new-dtype]
        (concat-buffers new-dtype (map #(dtype-proto/elemwise-reader-cast % new-dtype) [lhs rhs]))))))

(defn- as-prim-io ^Buffer [item] item)

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
  ^Buffer [datatype buffers]
  (let [counts (mapv dtype-proto/ecount buffers)
        ^List buffers (mapv dtype-proto/->buffer buffers)
        _ (assert (apply = counts))
        n-elems (long (apply + counts))
        buf-len (long (first counts))
        allowsRead (boolean (every? #(.allowsRead ^Buffer %) buffers))
        allowsWrite (boolean (every? #(.allowsWrite ^Buffer %) buffers))]
    (reify Buffer
      (elemwiseDatatype [this] datatype)
      (lsize [this] n-elems)
      (allowsRead [this] allowsRead)
      (allowsWrite [this] allowsWrite)
      (readLong [this idx] (same-len-read-macro idx n-elems buf-len .readLong buffers))
      (readDouble [this idx] (same-len-read-macro idx n-elems buf-len .readDouble buffers))
      (readObject [this idx] (same-len-read-macro idx n-elems buf-len .readObject buffers))
      (writeLong [this idx val] (same-len-write-macro idx n-elems buf-len .writeLong buffers val))
      (writeDouble [this idx val] (same-len-write-macro idx n-elems buf-len .writeDouble buffers val))
      (writeObject [this idx val] (same-len-write-macro idx n-elems buf-len .writeObject buffers val))
      dtype-proto/PElemwiseReaderCast
      (elemwise-reader-cast [this new-dtype]
        (concat-buffers new-dtype (map #(dtype-proto/elemwise-reader-cast % new-dtype) buffers))))))


(defmacro ^:private gen-read-macro
  [idx n-elems read-method _n-buffers buffers]
  `(do (errors/check-idx ~idx ~n-elems)
       (loop [buf-idx# 0
              idx# ~idx]
         (let [buffer# (as-prim-io (.get ~buffers buf-idx#))
               buf-len# (.lsize buffer#)]
           (if (< idx# buf-len#)
             (~read-method (as-prim-io buffer#) idx#)
             (recur (unchecked-inc buf-idx#) (pmath/- idx# buf-len#)))))))


(defmacro ^:private gen-write-macro
  [idx n-elems write-method _n-buffers buffers val]
  `(do (errors/check-idx ~idx ~n-elems)
       (loop [buf-idx# 0
              idx# ~idx]
         (let [buffer# (as-prim-io (.get ~buffers buf-idx#))
               buf-len# (.lsize buffer#)]
           (if (< idx# buf-len#)
             (~write-method (as-prim-io buffer#) idx# ~val)
             (recur (unchecked-inc buf-idx#) (pmath/- idx# buf-len#)))))))



(defn- generalized-concat-buffers
  ^Buffer [datatype buffers]
  (let [counts (mapv dtype-proto/ecount buffers)
        ^List buffers (mapv dtype-proto/->buffer buffers)
        n-elems (long (apply + counts))
        n-buffers (.size buffers)
        allowsRead (boolean (every? #(.allowsRead ^Buffer %) buffers))
        allowsWrite (boolean (every? #(.allowsWrite ^Buffer %) buffers))]
    (reify Buffer
      (elemwiseDatatype [this] datatype)
      (lsize [this] n-elems)
      (allowsRead [this] allowsRead)
      (allowsWrite [this] allowsWrite)
      (readLong [this idx] (gen-read-macro idx n-elems .readLong n-buffers buffers))
      (readDouble [this idx] (gen-read-macro idx n-elems .readDouble n-buffers buffers))
      (readObject [this idx] (gen-read-macro idx n-elems .readObject n-buffers buffers))
      (writeLong [this idx val] (gen-write-macro idx n-elems .writeLong n-buffers buffers val))
      (writeDouble [this idx val] (gen-write-macro idx n-elems .writeDouble n-buffers buffers val))
      (writeObject [this idx val] (gen-write-macro idx n-elems .writeObject n-buffers buffers val))
      dtype-proto/PElemwiseReaderCast
      (elemwise-reader-cast [this new-dtype]
        (concat-buffers new-dtype (map #(dtype-proto/elemwise-reader-cast % new-dtype) buffers))))))



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
                            (map dtype-proto/elemwise-datatype buffers))]
       (concat-buffers datatype buffers)))))
