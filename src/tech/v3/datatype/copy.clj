(ns tech.v3.datatype.copy
  (:require [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.packing :as packing]
            [ham-fisted.api :as hamf]
            [ham-fisted.reduce :as hamf-rf])
  (:import [sun.misc Unsafe]
           [ham_fisted ArrayLists$ArrayOwner]
           [tech.v3.datatype UnsafeUtil Buffer]
           [tech.v3.datatype ArrayHelpers Buffer$CopyingReducer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn unsafe
  "Get access to an instance of sun.misc.Unsafe."
  ^Unsafe []
  UnsafeUtil/unsafe)


(defonce error-on-generic-copy* (atom false))

(defn- passthrough-accum
  [acc v] acc)

(def passthrough-long-accum (hamf-rf/long-accumulator acc v acc))
(def passthrough-double-accum (hamf-rf/double-accumulator acc v acc))

(defn generic-copy!
  [src dst]
  (when @error-on-generic-copy*
    (errors/throwf "Generic copy detected!"))
  (let [dst-dtype (packing/unpack-datatype (dtype-base/elemwise-datatype dst))
        src (dtype-base/->reader src dst-dtype)
        dst (dtype-base/->writer dst)
        n-elems (.lsize src)]
    (when-not (== n-elems (.lsize dst))
      (throw (Exception. (format "src,dst ecount mismatch: %d-%d"
                                 n-elems (.lsize dst)))))
    (if (< n-elems 1024)
      (.fillRange dst 0 src)
      (hamf-rf/preduce (constantly nil)
                       (case (casting/simple-operation-space dst-dtype)
                         :int64 passthrough-long-accum
                         :float64 passthrough-double-accum
                         passthrough-accum)
                       passthrough-accum
                       (Buffer$CopyingReducer. src dst)))
    dst))


(defn unsafe-copy-memory
  "Only Arrays, arraybuffers, and native buffers implement the memcpy info protocol.
  If you know both sides are arrays it is faster to use System/arraycopy.
  datatypes, endianness, and ecounts  *must* match, this is not checked in this method.
  If in question, use [[high-perf-copy!]]."
  ([src-buf dst-buf src-dt ^long n-elems]
   (let [[src ^long src-off] (dtype-proto/memcpy-info src-buf)
         [dst ^long dst-off] (dtype-proto/memcpy-info dst-buf)]
     (if (and (nil? src) (nil? dst)
              (or (identical? :int8 src-dt)
                  (identical? :uint8 src-dt)))
       (UnsafeUtil/copyBytes src-off dst-off n-elems)
       (.copyMemory (unsafe)
                    src (long src-off)
                    dst (long dst-off)
                    (* n-elems (casting/numeric-byte-width src-dt))))
     dst-buf))
  ([src-buf dst-buf]
   (unsafe-copy-memory src-buf dst-buf
                       (dtype-proto/elemwise-datatype src-buf)
                       (dtype-base/ecount src-buf))))


(defn high-perf-copy!
  "Src, dst *must* be same unaliased datatype and that datatype must be a primitive
  datatype.
  src must either be convertible to an array or to a native buffer.
  dst must either be convertible to an array or to a native buffer.
  Uses Unsafe/copyMemory under the covers *without* safePointPolling.
  Returns dst"
  [src dst]
  (let [src-dt (casting/host-flatten (dtype-base/elemwise-datatype src))
        src-ec (dtype-base/ecount src)
        dst-ec (dtype-base/ecount dst)
        n-elems src-ec]
    (when-not (== src-ec dst-ec)
      (throw (Exception. (format "src ecount (%s) != dst ecount (%s)"
                                 src-ec dst-ec))))
    (if (identical? src-dt (casting/host-flatten (dtype-base/elemwise-datatype dst)))
      (let [src-buf (dtype-base/as-concrete-buffer src)
            dst-buf (dtype-base/as-concrete-buffer dst)]
        (cond
          (and (instance? ArrayLists$ArrayOwner src-buf)
               (instance? ArrayLists$ArrayOwner dst-buf))
          (let [src (.getArraySection ^ArrayLists$ArrayOwner src-buf)
                dst (.getArraySection ^ArrayLists$ArrayOwner dst-buf)]
            (System/arraycopy (.array src) (.sidx src)
                              (.array dst) (.sidx dst)
                              (.size src)))
          (and src-buf dst-buf
               (identical? (dtype-proto/endianness src-buf)
                           (dtype-proto/endianness dst-buf)))
          (unsafe-copy-memory src-buf dst-buf src-dt n-elems)
          :else
          (generic-copy! (or src-buf src)
                         (or dst-buf dst))))

      (generic-copy! src dst))
    dst))


(defn copy!
  ([src dst _ignored] (copy! src dst))
  ([src dst]
   (if (dtype-proto/convertible-to-reader? src)
     (high-perf-copy! src dst)
     (let [op-space (casting/simple-operation-space (dtype-proto/elemwise-datatype dst))
           ^Buffer dst-buf (dtype-base/->writer dst)
           rfn (case op-space
                 :int64 (hamf-rf/indexed-long-accum
                         acc idx v
                         (.writeLong ^Buffer acc idx v) acc)
                 :float64 (hamf-rf/indexed-double-accum
                           acc idx v
                           (.writeDouble ^Buffer acc idx v) acc)
                 (hamf-rf/indexed-accum
                  acc idx v
                  (.writeObject ^Buffer acc idx v) acc))]
       (when-not (== (count src) (dtype-base/ecount dst))
         (throw (RuntimeException. "src,dst ecounts do not match")))
       (reduce rfn dst-buf src)
       dst-buf))))
