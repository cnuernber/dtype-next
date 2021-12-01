(ns tech.v3.tensor.tensor-copy
  (:require [tech.v3.datatype.index-algebra :as idx-alg]
            [tech.v3.tensor.dimensions.global-to-local :as gtol]
            [tech.v3.tensor.dimensions.analytics :as dims-analytics]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.copy :as dtype-copy]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.errors :as errors])
  (:import [tech.v3.datatype Buffer NDBuffer]
           [java.util List]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn setup-dims-bit-blit
  [src-dims dst-dims]
  (let [src-offsets? (dims-analytics/any-offsets? src-dims)
        dst-offsets? (dims-analytics/any-offsets? dst-dims)
        src-dims (dims-analytics/reduce-dimensionality src-dims src-offsets?)
        dst-dims (dims-analytics/reduce-dimensionality dst-dims dst-offsets?)
        ^List src-shape (:shape src-dims)
        ^List src-strides (:strides src-dims)
        ^List src-max-shape (:shape-ecounts src-dims)
        n-src (.size src-shape)
        n-src-dec (dec n-src)
        ^List dst-shape (:shape dst-dims)
        ^List dst-strides (:strides dst-dims)
        ^List dst-max-shape (:shape-ecounts dst-dims)
        n-dst (.size dst-shape)
        n-dst-dec (dec n-dst)

        both-direct? (and (number? (.get src-shape n-src-dec))
                          (number? (.get dst-shape n-dst-dec)))
        strides-packed? (and (== 1 (long (.get src-strides n-src-dec)))
                             (== 1 (long (.get dst-strides n-dst-dec))))
        src-last-offset (long (if src-offsets?
                                (idx-alg/get-offset
                                 (.get src-shape n-src-dec))
                                0))
        dst-last-offset (long
                         (if dst-offsets?
                           (idx-alg/get-offset
                            (.get dst-shape n-dst-dec))
                           0))]
    (when (and both-direct?
               strides-packed?
               (== 0 src-last-offset)
               (== 0 dst-last-offset))
      (let [dst-last-shape (long (.get dst-shape n-dst-dec))
            src-last-shape (long (.get src-shape n-src-dec))
            min-shape (min dst-last-shape src-last-shape)
            max-shape (max dst-last-shape src-last-shape)]
        ;;the shapes have to be commensurate
        (when (and (== 0 (rem max-shape min-shape))
                   (== dst-last-shape (long (.get dst-max-shape n-dst-dec)))
                   (== src-last-shape (long (.get src-max-shape n-src-dec))))
          (let [src-reader (gtol/reduced-dims->global->local-reader src-dims)
                dst-reader (gtol/reduced-dims->global->local-reader dst-dims)]
            {:block-size min-shape
             :n-blocks (quot (.lsize src-reader)
                             min-shape)
             :src-offset-reader src-reader
             :dst-offset-reader dst-reader}))))))


(defn bit-blit!
  "Returns :ok if bit blit succeeds"
  ([^NDBuffer src ^NDBuffer dst options]
   (errors/when-not-errorf (and (instance? NDBuffer dst)
                                (instance? NDBuffer src))
     "Both src (%s) and dst (%s) must be tensors"
     (type src) (type dst))
   (errors/when-not-errorf (= (.shape dst)
                              (.shape src))
     "Src shape (%s) differs from dst shape (%s)"
     (.shape src)
     (.shape dst))
   (let [unchecked? (:unchecked? options)
         src-dtype (dtype-base/elemwise-datatype src)
         dst-dtype (dtype-base/elemwise-datatype dst)
         src-dtype (if unchecked?
                     (casting/host-flatten src-dtype)
                     src-dtype)
         dst-dtype (if unchecked?
                     (casting/host-flatten dst-dtype)
                     dst-dtype)
         ;;Getting the buffers defeats a check in the tensors
         src-buffer (when (.buffer src) (dtype-base/as-buffer (.buffer src)))
         dst-buffer (when (.buffer dst) (dtype-base/as-buffer (.buffer dst)))]
     (when (and src-buffer
                dst-buffer
                (= src-dtype
                   dst-dtype))
       (when-let [dims-data (setup-dims-bit-blit
                             (.dimensions src)
                             (.dimensions dst))]
         (let [block-size (long (:block-size dims-data))
               n-blocks (long (:n-blocks dims-data))
               ^Buffer src-offset-reader (:src-offset-reader dims-data)
               ^Buffer dst-offset-reader (:dst-offset-reader dims-data)]
           (when (>= block-size 512)
             (parallel-for/parallel-for
              idx n-blocks
              (let [offset (* idx block-size)
                    src-offset (.readLong src-offset-reader offset)
                    dst-offset (.readLong dst-offset-reader offset)]
                (dtype-copy/copy! (dtype-proto/sub-buffer src-buffer src-offset
                                                          block-size)
                                  (dtype-proto/sub-buffer dst-buffer dst-offset
                                                          block-size)
                                  unchecked?)))
             :ok))))))
  ([src dst]
   (bit-blit! src dst {})))


(defn tensor-copy!
  [src dst options]
  (when-not (bit-blit! src dst options)
    (dtype-cmc/copy! src dst options))
  dst)
