(ns tech.v3.datatype.immap
  "Fast immutable map implementation for a fixed number of key/vals."
  (:require [tech.v3.datatype.imlist :as imlist])
  (:import [clojure.lang Util Murmur3 IHashEq IPersistentCollection
            IPersistentMap IteratorSeq IObj APersistentMap MapEquivalence MapEntry]
           [java.util Map List Objects Map$Entry RandomAccess AbstractSet]
           [java.util.function Predicate]
           [tech.v3.datatype SimpleSet IFnDef]))


(declare immap immap-meta)

(defmacro ^:private throw-unimplemented
  []
  `(throw (UnsupportedOperationException.)))

(defn- as-entry
  ^Map$Entry [item] item)


(defn ^:no-doc map-hasheq
  ^long [^Map data]
  (Murmur3/hashUnordered data))

(defn ^:no-doc map-hashcode
  ^long [^IPersistentMap data]
  (APersistentMap/mapHash data))

(defn ^:no-doc map-equiv
  [^IPersistentMap lhs obj]
  (if (not (instance? Map obj))
    false
    (if (and (instance? IPersistentMap obj)
             (not (instance? MapEquivalence obj)))
      false
      (let [^Map m obj]
        (if (not (== (.size lhs) (.size m)))
          false
          (let [ks (.keySet lhs)
                iter (.iterator ^Iterable ks)]
            (loop [more? (.hasNext iter)]
              (if more?
                (let [k (.next iter)]
                  (if (and (.containsKey m k)
                           (Util/equiv (.get lhs k) (.get m k)))
                    (recur (.hasNext iter))
                    false))
                true))))))))


(defn ^:no-doc map-equals
  [^IPersistentMap lhs obj]
  (APersistentMap/mapEquals lhs obj))


(defmacro ^:private define-immap
  [argc]
  (let [argc (long argc)
        tname (symbol (str "Immap" argc))
        argvec (->> (range argc)
                    (mapcat (fn [idx]
                              [(symbol (str "argk" idx))
                               (symbol (str "argv" idx))]))
                    (vec))
        keyvec (mapv first (partition 2 argvec))
        valvec (mapv second (partition 2 argvec))
        hcsym (with-meta (symbol "hash-code")
                {:unsynchronized-mutable true
                 :tag 'int})
        hesym (with-meta (symbol "hash-eq")
                {:unsynchronized-mutable true
                 :tag 'int})]
    `(deftype ~tname ~(apply vector hcsym hesym 'metadata argvec)
       IHashEq
       (hasheq [this#]
         (if (== 0 ~'hash-eq)
           (set! ~'hash-eq (unchecked-int (map-hasheq this#)))
           ~'hash-eq))
       MapEquivalence
       IPersistentCollection
       (count [this#] ~argc)
       (cons [this# val#]
         (cond
           (instance? Map$Entry val#)
           (let [val# (as-entry val#)]
             (.assoc this# (.getKey val#) (.getValue val#)))
           (instance? RandomAccess val#)
           (let [^List val# val#]
             (when-not (== 2 (.size val#))
               (throw (Exception. "Vector entries must have length 2")))
             (.assoc this# (.get val# 0) (.get val# 1)))
           :else
           (throw (Exception. (str "Unrecognized cons value: " val#)))))
       (empty [this#] {})
       (equiv [this# o#] (boolean (map-equiv this# o#)))
       (seq [this#] (IteratorSeq/create (.iterator this#)))
       IPersistentMap
       (assoc [this# ~'k ~'v]
         (cond
           ~@(->> (range argc)
                  (mapcat
                   (fn [argidx]
                     [`(= ~'k ~(keyvec argidx))
                      `(immap-meta ~'metadata
                                     ~@(concat
                                        (subvec argvec 0 (* 2 argidx))
                                        ['k 'v]
                                        (subvec argvec (* 2 (inc argidx)) (* 2 argc))))])))
           :else (immap-meta ~'metadata ~@argvec ~'k ~'v)))
       (assocEx [this# key# val#]
         (when-not (.containsKey this# key#)
           (throw (Exception. (str "Duplicate key: " key#))))
         (.assoc this# key# val#))
       (without [this# ~'k]
         (cond
           ~@(->> (range argc)
                  (mapcat
                   (fn [argidx]
                     [`(= ~'k ~(keyvec argidx))
                      `(immap-meta ~'metadata
                                     ~@(concat
                                        (subvec argvec 0 (* 2 argidx))
                                        (subvec argvec (* 2 (inc argidx)) (* 2 argc))))])))
           :else this#))
       Map
       (size [this#] ~argc)
       (keySet [this#]
         (let [ks# (imlist/imlist ~@keyvec)]
           (SimpleSet. ~argc ks# (reify Predicate
                                   (test [p# o#]
                                     (.containsKey this# o#))))))
       (entrySet [this#]
         (let [kvs# (imlist/imlist ~@(map (fn [ksym valsm]
                                            `(MapEntry. ~ksym ~valsm))
                                          keyvec valvec))]
           (SimpleSet. ~argc kvs# (reify Predicate
                                    (test [p# o#]
                                      (if (instance? Map$Entry o#)
                                        (let [^Map$Entry entry# o#]
                                          (Objects/equals (.getValue entry#)
                                                          (.get this# (.getKey entry#))))
                                        ))))))
       (values [this#] (imlist/imlist ~@valvec))
       (containsKey [this# ~'k]
         (cond
           ~@(->> keyvec
                  (mapcat (fn [ksym]
                            [`(= ~'k ~ksym) true])))
           :else
           false))
       (get [this# ~'k]
         (cond
           ~@(->> (range argc)
                  (mapcat (fn [kidx]
                            [`(= ~'k ~(keyvec kidx)) (valvec kidx)])))))
       (getOrDefault [this# ~'k defval#]
         (cond
           ~@(->> (range argc)
                  (mapcat (fn [kidx]
                            [`(= ~'k ~(keyvec kidx)) (valvec kidx)])))
           :else
           defval#))
       (put [this# k# v#] (throw-unimplemented))
       Iterable
       (iterator [this#]
         (let [ks# (imlist/imlist ~@(->> (range argc)
                                         (map (fn [idx]
                                                `(MapEntry. ~(keyvec idx) ~(valvec idx))))))]
           (.iterator ^Iterable ks#)))
       IObj
       (meta [this#] ~'metadata)
       (withMeta [this# newmeta#]
         (new ~tname ~'hash-code ~'hash-eq newmeta# ~@argvec))
       IFnDef
       (invoke [this# k#] (.get this# k#))
       Object
       (hashCode [this#]
         (if (== 0 ~'hash-code)
           (set! ~'hash-code (unchecked-int (map-hashcode this#)))
           ~'hash-code))
       (equals [this# o#]
         (map-equals this# o#))
       (toString [this#]
         (.toString ^Object (into {} (seq this#)))))))


(define-immap 1)
(define-immap 2)
(define-immap 3)
(define-immap 4)
(define-immap 5)
(define-immap 6)


(defn immap-meta
  ([meta] (with-meta {} meta))
  ([meta k v] (Immap1. 0 0 meta k v))
  ([meta k0 v0 k1 v1] (Immap2. 0 0 meta k0 v0 k1 v1))
  ([meta k0 v0 k1 v1 k2 v2] (Immap3. 0 0 meta k0 v0 k1 v1 k2 v2))
  ([meta k0 v0 k1 v1 k2 v2 k3 v3] (Immap4. 0 0 meta k0 v0 k1 v1 k2 v2 k3 v3))
  ([meta k0 v0 k1 v1 k2 v2 k3 v3 k4 v4] (Immap5. 0 0 meta k0 v0 k1 v1 k2 v2 k3 v3 k4 v4))
  ([meta k0 v0 k1 v1 k2 v2 k3 v3 k4 v4 k5 v5] (Immap6. 0 0 meta k0 v0 k1 v1 k2 v2 k3 v3 k4 v4
                                                  k5 v5))
  ([meta k0 v0 k1 v1 k2 v2 k3 v3 k4 v4 k5 v5 & args]
   (apply hash-map k0 v0 k1 v1 k2 v2 k3 v3 k4 v4
          k5 v5 args)))


(defn immap
  ([] {})
  ([k v] (Immap1. 0 0 nil k v))
  ([k0 v0 k1 v1] (Immap2. 0 0 nil k0 v0 k1 v1))
  ([k0 v0 k1 v1 k2 v2] (Immap3. 0 0 nil k0 v0 k1 v1 k2 v2))
  ([k0 v0 k1 v1 k2 v2 k3 v3] (Immap4. 0 0 nil k0 v0 k1 v1 k2 v2 k3 v3))
  ([k0 v0 k1 v1 k2 v2 k3 v3 k4 v4] (Immap5. 0 0 nil k0 v0 k1 v1 k2 v2 k3 v3 k4 v4))
  ([k0 v0 k1 v1 k2 v2 k3 v3 k4 v4 k5 v5] (Immap6. 0 0 nil k0 v0 k1 v1 k2 v2 k3 v3 k4 v4
                                                  k5 v5))
  ([k0 v0 k1 v1 k2 v2 k3 v3 k4 v4 k5 v5 & args]
   (into {} (concat [k0 v0 k1 v1 k2 v2 k3 v3 k4 v4 k5 v5] args))))
