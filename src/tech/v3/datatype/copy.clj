(ns tech.v3.datatype.copy
  (:require [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [sun.misc Unsafe]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype NDBuffer]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn generic-copy!
  [src dst]
  (let [dst-dtype (dtype-base/elemwise-datatype dst)
        op-space (casting/simple-operation-space dst-dtype)
        src (dtype-base/->reader src dst-dtype)
        dst (dtype-base/->writer dst)
        n-elems (.lsize src)]
    (when-not (== n-elems (.lsize dst))
      (throw (Exception. (format "src,dst ecount mismatch: %d-%d"
                                 n-elems (.lsize dst)))))
    (case op-space
      :boolean
      (parallel-for/parallel-for
       idx
       n-elems
       (.writeBoolean dst idx (.readBoolean src idx)))
      :int64
      (parallel-for/parallel-for
       idx
       n-elems
       (.writeLong dst idx (.readLong src idx)))
      :float64
      (parallel-for/parallel-for
       idx
       n-elems
       (.writeDouble dst idx (.readDouble src idx)))
      :object
      (parallel-for/parallel-for
       idx
       n-elems
       (.writeObject dst idx (.readObject src idx))))
    dst))


(defn- array-base-offset
  ^long [ary]
  (case (dtype-base/elemwise-datatype ary)
    :boolean Unsafe/ARRAY_BOOLEAN_BASE_OFFSET
    :int8 Unsafe/ARRAY_BYTE_BASE_OFFSET
    :int16 Unsafe/ARRAY_SHORT_BASE_OFFSET
    :char Unsafe/ARRAY_CHAR_BASE_OFFSET
    :int32 Unsafe/ARRAY_INT_BASE_OFFSET
    :int64 Unsafe/ARRAY_LONG_BASE_OFFSET
    :float32 Unsafe/ARRAY_FLOAT_BASE_OFFSET
    :float64 Unsafe/ARRAY_DOUBLE_BASE_OFFSET
    Unsafe/ARRAY_OBJECT_BASE_OFFSET))


(defn- unpack-copy-item
  [item]
  (cond
    (instance? NativeBuffer item)
    ;;no further offsetting required for native buffers
    [nil (.address ^NativeBuffer item)]
    (instance? ArrayBuffer item)
    (let [ary-buf ^ArrayBuffer item
          ary (.ary-data ary-buf)
          ary-off (* (.offset ary-buf)
                     (casting/numeric-byte-width (.elemwise-datatype ary-buf)))]
      [ary (+ ary-off (array-base-offset ary))])
    (array-buffer/is-array-type? item)
    (array-base-offset item)
    :else
    (throw (Exception. (format "Invalid item in unpack-copy-item: %s"
                               (type item))))))


(defn high-perf-copy!
  "Src, dst *must* be same unaliased datatype and that datatype must be a primitive
  datatype.
  src must either be convertible to an array or to a native buffer.
  dst must either be convertible to an array or to a native buffer.
  Uses Unsafe/copyMemory under the covers *without* safePointPolling.
  Returns dst"
  ([src dst n-elems]
   (let [src-dt (casting/host-flatten (dtype-base/elemwise-datatype src))
         src-ec (dtype-base/ecount src)
         dst-ec (dtype-base/ecount dst)
         n-elems (long n-elems)]
     (when-not (= src-dt (casting/host-flatten (dtype-base/elemwise-datatype dst)))
       (throw (Exception. (format "src dtype (%s) != dst dtype (%s)"
                                  src-dt
                                  (casting/host-flatten
                                   (dtype-base/elemwise-datatype dst))))))
     (when-not (== src-ec dst-ec)
       (throw (Exception. (format "src ecount (%s) != dst ecount (%s)"
                                  src-ec dst-ec))))
     ;;Check if managed heap or native heap
     (let [src-buf (or (dtype-base/as-array-buffer src)
                       (dtype-base/as-native-buffer src))
           dst-buf (or (dtype-base/as-array-buffer dst)
                       (dtype-base/as-native-buffer dst))
           _ (when-not (and src dst)
               (throw (Exception.
                       "Src or dst are not convertible to arrays or native buffers")))]
       (cond
         (and (instance? ArrayBuffer src-buf)
              (instance? ArrayBuffer dst-buf))
         (let [^ArrayBuffer src src-buf
               ^ArrayBuffer dst dst-buf]
           (if (< n-elems 1024)
             (System/arraycopy (.ary-data src) (.offset src)
                               (.ary-data dst) (.offset dst)
                               (.n-elems src))
             ;;Parallelize the copy op.
             (parallel-for/indexed-map-reduce
              n-elems
              (fn [^long start-idx ^long group-len]
                (System/arraycopy (.ary-data src) (+ (.offset src) start-idx)
                                  (.ary-data dst) (+ (.offset dst) start-idx)
                                  group-len)))))
         (= (dtype-proto/endianness src-buf)
            (dtype-proto/endianness dst-buf))
         (let [[src src-off] (unpack-copy-item src-buf)
               [dst dst-off] (unpack-copy-item dst-buf)
               byte-width (casting/numeric-byte-width
                           (casting/un-alias-datatype src-dt))]
           (if (< n-elems 1024)
             (.copyMemory (native-buffer/unsafe)
                          src (long src-off)
                          dst (long dst-off)
                          (* n-elems byte-width))
             (parallel-for/indexed-map-reduce
              n-elems
              (fn [^long start-idx ^long group-len]
                (let [[src src-off] (unpack-copy-item
                                     (dtype-base/sub-buffer src-buf
                                                            start-idx group-len))
                      [dst dst-off] (unpack-copy-item
                                     (dtype-base/sub-buffer dst-buf
                                                            start-idx group-len))]
                  (.copyMemory (native-buffer/unsafe)
                               src (long src-off)
                               dst (long dst-off)
                               (* group-len byte-width)))))))
         :else
         (generic-copy! src dst))
       dst)))
  ([src dst]
   (let [src-ec (dtype-base/ecount src)
         dst-ec (dtype-base/ecount dst)]
     (when-not (== src-ec dst-ec)
       (throw (Exception. (format "src ecount (%s) != dst ecount (%s)"
                                  src-ec dst-ec))))
     (high-perf-copy! src dst src-ec))))


(defn copy!
  ([src dst unchecked?]
   (let [src-dtype (dtype-base/elemwise-datatype src)
         dst-dtype (dtype-base/elemwise-datatype dst)
         equal-dtype? (if unchecked?
                        (= (casting/host-flatten src-dtype)
                           (casting/host-flatten dst-dtype))
                        (= src-dtype dst-dtype))
         src-buf (or (dtype-base/as-array-buffer src)
                     (dtype-base/as-native-buffer src))
         dst-buf (or (dtype-base/as-array-buffer dst)
                     (dtype-base/as-native-buffer dst))]
     (when-not (== (dtype-base/ecount src) (dtype-base/ecount dst))
       (throw (Exception. (format "Elem counts differ: src: %d, dst: %d"
                                  (dtype-base/ecount src) (dtype-base/ecount dst)))))
     (cond
       (and equal-dtype? src-buf dst-buf)
       (high-perf-copy! src-buf dst-buf)
       (dtype-base/->reader src)
       (generic-copy! src dst)
       :else
       (do
         (when-not (instance? Iterable src)
           (throw (Exception. "Src must be either convertible to reader or iterable")))
         (let [iter (.iterator ^Iterable src)
              writer (dtype-base/->writer dst)
               cast-fn (if unchecked?
                         (@casting/*unchecked-cast-table* dst-dtype)
                         (@casting/*cast-table* dst-dtype))]
          ;;and off we go
          (loop [continue? (.hasNext iter)
                 idx 0]
            (when continue?
              (.writeObject writer idx (cast-fn (.next iter)))
              (recur (.hasNext iter) (unchecked-inc idx)))))))
     dst))
  ([src dst]
   (copy! src dst false)))
