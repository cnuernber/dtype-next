(ns tech.v3.tensor.dimensions.global-to-local
  "Given a generic description object, return an interface that can efficiently
  transform indexes in global coordinates mapped to local coordinates."
  (:require [tech.v3.tensor.dimensions.analytics :as dims-analytics]
            [tech.v3.datatype.index-algebra :as idx-alg]
            [tech.v3.datatype.graal-native :as graal-native]
            [com.github.ztellman.primitive-math :as pmath]
            [clojure.tools.logging :as log]
            [camel-snake-kebab.core])
  (:import [tech.v3.datatype Buffer LongReader LongNDReader]
           [java.util.function Function]
           [java.util List]
           [java.util.concurrent ConcurrentHashMap]))


(set! *unchecked-math* :warn-on-boxed)
(set! *warn-on-reflection* true)


(defn elem-idx->addr-fn
  "Generic implementation of global->local transformation."
  ^Buffer [reduced-dims]
  (try
    (let [^objects shape (object-array (:shape reduced-dims))
          ^longs strides (long-array (:strides reduced-dims))
          ^longs offsets (when-not (every? #(== 0 (long %)) (:offsets reduced-dims))
                           (long-array (:offsets reduced-dims)))
          ^longs max-shape (long-array (:shape-ecounts reduced-dims))
          ^longs max-shape-strides (long-array (:shape-ecount-strides reduced-dims))
          n-dims (alength shape)
          n-elems (pmath/* (aget max-shape-strides 0)
                           (aget max-shape 0))]
      ;;With everything typed correctly, this pathway is actually amazingly fast.
      (if offsets
        (reify LongReader
          (lsize [rdr] n-elems)
          (readLong [rdr idx]
            (loop [dim 0
                   result 0]
              (if (< dim n-dims)
                (let [shape-val (aget shape dim)
                      offset (aget offsets dim)
                      idx (pmath/+
                           (pmath// idx (aget max-shape-strides dim))
                           offset)
                      stride (aget strides dim)
                      local-val (if (number? shape-val)
                                  (-> (pmath/rem idx (long shape-val))
                                      (pmath/* stride))
                                  (-> (.readLong ^Buffer shape-val
                                                 (pmath/rem idx
                                                            (.lsize ^Buffer shape-val)))
                                      (pmath/* stride)))]
                  (recur (pmath/inc dim) (pmath/+ result local-val)))
                result))))
        (reify LongReader
          (lsize [rdr] n-elems)
          (readLong [rdr idx]
            (loop [dim 0
                   result 0]
              (if (< dim n-dims)
                (let [shape-val (aget shape dim)
                      idx (pmath// idx (aget max-shape-strides dim))
                      stride (aget strides dim)
                      local-val (if (number? shape-val)
                                  (-> (pmath/rem idx (long shape-val))
                                      (pmath/* stride))
                                  (-> (.readLong ^Buffer shape-val
                                                 (pmath/rem idx
                                                            (.lsize ^Buffer shape-val)))
                                      (pmath/* stride)))]
                  (recur (pmath/inc dim) (pmath/+ result local-val)))
                result))))))
    (catch Throwable e
      (log/errorf e "Failed to produce idx->addr fn for reduced dimensions %s"
                  (pr-str reduced-dims))
      (throw e))))


(defn reduced-dims->signature
  ([{:keys [shape strides offsets _shape-ecounts _shape-ecount-strides]} broadcast?]
   (let [n-dims (count shape)
         direct-vec (mapv idx-alg/direct-reader? shape)
         offsets? (boolean (some #(not= 0 %) offsets))
         trivial-last-stride? (== 1 (long (.get ^List strides (dec n-dims))))]
     {:n-dims n-dims
      :direct-vec direct-vec
      :offsets? offsets?
      :broadcast? broadcast?
      :trivial-last-stride? trivial-last-stride?}))
  ([reduced-dims]
   (reduced-dims->signature reduced-dims
                            (dims-analytics/are-reduced-dims-bcast?
                             reduced-dims))))


#_(graal-native/if-defined-graal-native
 (log/debug "Graal Native Defined -- insn custom indexing disabled!")
 (do
   (log/debug "insn custom indexing enabled!")
   (defonce ^ConcurrentHashMap defined-classes (ConcurrentHashMap.))

   (def sig->constructor-fn
     (try
       (let [insn-fn
             (requiring-resolve 'tech.v3.tensor.dimensions.gtol-insn/generate-constructor)]
         (fn [signature]
           (try
             (insn-fn signature)
             (catch Throwable e
               (log/warnf e "Index function generation failed for sig %s" signature)
               elem-idx->addr-fn))))
       (catch Throwable e
         (log/warn e "insn unavailable-falling back to default indexing system")
         elem-idx->addr-fn)))


   (defn- absent-sig-fn
     [signature]
     (reify Function
       (apply [this signature]
         (sig->constructor-fn signature))))))


(defn make-indexing-obj
  [reduced-dims broadcast?]
  #_(graal-native/if-defined-graal-native
   (elem-idx->addr-fn reduced-dims)
   (let [signature (reduced-dims->signature reduced-dims broadcast?)
         reader-constructor-fn
         (or (.get defined-classes signature)
             (.computeIfAbsent
              defined-classes
              signature
              (absent-sig-fn signature)))]
     (reader-constructor-fn reduced-dims)))
  (elem-idx->addr-fn reduced-dims))


(defn get-or-create-reader
  (^Buffer [reduced-dims broadcast? force-default-reader?]
   (let [n-dims (count (:shape reduced-dims))]
     (if (and (not force-default-reader?)
              (<= n-dims 4))
       (make-indexing-obj reduced-dims broadcast?)
       (elem-idx->addr-fn reduced-dims))))
  (^Buffer [reduced-dims]
   (get-or-create-reader reduced-dims
                         (dims-analytics/are-reduced-dims-bcast? reduced-dims)
                         false)))


(defn dims->global->local-reader
  ^Buffer [dims]
  (-> (dims-analytics/dims->reduced-dims dims)
      (get-or-create-reader)))


(defn reduced-dims->global->local-reader
  ^Buffer [reduced-dims]
  (get-or-create-reader reduced-dims))


(defn dims->global->local
  ^LongNDReader [dims]
  (let [shape-ecounts (long-array (:shape-ecounts dims))
        shape-ecount-strides (long-array (:shape-ecount-strides dims))
        n-dims (alength shape-ecount-strides)
        n-dims-dec (dec n-dims)
        n-dims-dec-1 (max 0 (dec n-dims-dec))
        n-dims-dec-2 (max 0 (dec n-dims-dec-1))
        elemwise-reader (dims->global->local-reader dims)
        n-elems (.lsize elemwise-reader)

        ;;xyz->global row major index calculation
        shape-ecount-strides-dec-1 (aget shape-ecount-strides n-dims-dec-1)
        shape-ecount-strides-dec-2 (aget shape-ecount-strides n-dims-dec-2)
        rank n-dims
        outermostDim (long (first shape-ecounts))]
    ;;Important common case optimization, native dimensions do not need
    ;;index translations.
    (if (:native? dims)
      (reify LongNDReader
        (shape [rdr] (:shape-ecounts dims))
        (lsize [rdr] n-elems)
        (rank [rdr] rank)
        (outermostDim [rdr] outermostDim)
        (readLong [rdr idx] idx)
        (ndReadLong [rdr idx] idx)
        (ndReadLong[this row col]
          (pmath/+ (pmath/* row shape-ecount-strides-dec-1) col))
        (ndReadLong [this height width chan]
          (pmath/+
           (pmath/* height shape-ecount-strides-dec-2)
           (pmath/* width shape-ecount-strides-dec-1)
           chan))
        (ndReadLongIter [this dims]
          (let [iter (.iterator dims)]
            (loop [continue? (.hasNext iter)
                   val 0
                   idx 0]
              (if continue?
                (let [next-val (long (.next iter))]
                  (recur (.hasNext iter)
                         (-> (* next-val (aget shape-ecount-strides idx))
                             (pmath/+ val))
                         (pmath/inc idx)))
                val)))))
      (reify LongNDReader
        (shape [rdr] (:shape-ecounts dims))
        (lsize [rdr] n-elems)
        (rank [rdr] rank)
        (outermostDim [rdr] outermostDim)
        (readLong [rdr idx]
          (.readLong elemwise-reader idx))
        (ndReadLong [rdr idx]
          (.readLong elemwise-reader idx))
        (ndReadLong[this row col]
          (.readLong elemwise-reader
                     (pmath/+ (pmath/* row shape-ecount-strides-dec-1)
                              col)))
        (ndReadLong [this height width chan]
          (.readLong elemwise-reader
                     (pmath/+
                      (pmath/* height shape-ecount-strides-dec-2)
                      (pmath/* width shape-ecount-strides-dec-1)
                      chan)))
        (ndReadLongIter [this dims]
          (let [iter (.iterator dims)]
            (.readLong elemwise-reader
                       (loop [continue? (.hasNext iter)
                              val 0
                              idx 0]
                         (if continue?
                           (let [next-val (long (.next iter))]
                             (recur (.hasNext iter)
                                    (-> (* next-val (aget shape-ecount-strides idx))
                                        (pmath/+ val))
                                    (pmath/inc idx)))
                           val)))))))))
