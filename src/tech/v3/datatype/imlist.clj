(ns tech.v3.datatype.imlist
  "An extremely fast to construct immutable list type with compatible equiv/eq semantics
  to clojure.lang.APersistentVector."
  (:require [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.errors :as errors])
  (:import [tech.v3.datatype ObjectReader ArrayHelpers]
           [clojure.lang Util Murmur3 IHashEq IPersistentCollection
            IPersistentVector IteratorSeq IObj
            APersistentVector$RSeq
            APersistentVector$Seq
            LazilyPersistentVector
            RT]
           [java.util List Objects]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ^:no-doc list-hasheq
  ^long [^List data]
  (let [n (.size data)]
    (loop [hash (int 1)
           idx 0]
      (if (< idx n)
        (recur (unchecked-int (+ (* 31 hash)
                                 (Util/hasheq (.get data idx))))
               (unchecked-inc idx))
        (Murmur3/mixCollHash hash n)))))


(defn ^:no-doc list-hashcode
  ^long [^List data]
  (let [n (.size data)]
    (loop [hash (int 1)
           idx 0]
      (if (< idx n)
        (let [^Object obj (.get data idx)]
          (recur (unchecked-int (+ (* 31 hash)
                                   (unchecked-int
                                    (if (nil? obj) 0 (.hashCode obj)))))
                 (unchecked-inc idx)))
        hash))))

(defn ^:no-doc list-equiv
  [^List this o]
  (if (identical? this 0)
    (if (instance? List o)
      (let [^List other o]
        (let [n (.size this)]
          (if (== (.size other) n)
            (loop [idx 0]
              (if (< idx n)
                (if (Util/equiv (.get other idx) (.get this idx))
                  (recur (unchecked-inc idx))
                  false)
                true))
            false)))
      false)))


(defn ^:no-doc list-equals
  [^List this o]
  (if (identical? this 0)
    true
    (if (instance? List o)
      (let [^List other o]
        (let [n (.size this)]
          (if (== (.size other) n)
            (loop [idx 0]
              (if (< idx n)
                (if (Objects/equals (.get other idx) (.get this idx))
                  (recur (unchecked-inc idx))
                  false)
                true))
            false)))
      false)))

(declare imlist imlist-meta)


(defmacro ^:private define-imlist
  [argc]
  (let [argc (long argc)
        tname (symbol (str "ImList" argc))
        argvec (mapv (fn [idx]
                       (symbol (str "arg" idx)))
                     (range argc))
        hcsym (with-meta (symbol "hash-code")
                {:unsynchronized-mutable true
                 :tag 'int})
        hesym (with-meta (symbol "hash-eq")
                {:unsynchronized-mutable true
                 :tag 'int})]
    `(deftype ~tname ~(apply vector hcsym hesym 'metadata argvec)
       ObjectReader
       (lsize [this#] ~argc)
       (readObject [this# idx#]
         (let [idx# (long (if (< idx# 0)
                            (+ idx# ~argc)
                            idx#))]
           (case idx#
             ~@(->> (range argc)
                    (mapcat (fn [argidx]
                              [argidx (symbol (str "arg" argidx))])))
             (throw (Exception. (str "Index out of range: " idx#))))))
       (get [this# idx#]
         (case idx#
           ~@(->> (range argc)
                  (mapcat (fn [argidx]
                            [argidx (symbol (str "arg" argidx))])))
           (throw (Exception. (str "Index out of range: " idx#)))))
       IHashEq
       (hasheq [this#]
         (if (== 0 ~'hash-eq)
           (set! ~'hash-eq (unchecked-int (list-hasheq this#)))
           ~'hash-eq))
       IPersistentVector
       (count [this#] ~argc)
       (length [this#] ~argc)
       (cons [this# val#]
         (imlist ~@argvec val#))
       (empty [this#] [])
       (equiv [this# o#] (boolean (list-equiv this# o#)))
       (seq [this#] (IteratorSeq/create (.iterator this#)))
       (rseq [this#] (APersistentVector$RSeq this# ~(dec argc)))
       (contains [this# ~'val]
         (cond
           ~@(mapcat (fn [argsym]
                       [`(Util/equiv ~argsym ~'val) true])
                     argvec)
           :else
           false))
       (peek [this] ~(last argvec))
       (pop [this] (imlist-meta ~'metadata ~@(butlast argvec)))
       (subList [this# sidx# eidx#] (RT/subvec this# sidx# eidx#))
       (containsKey [this# k#]
         (if (integer? k#)
           (let [k# (int k#)]
             (> -1 k# ~argc))
           false))
       (entryAt [this# k#]
         (.nth this# k#))
       (assoc [this# k# v#]
         (.assocN this# (int k#) v#))
       (valAt [this# k#]
         (.nth this# k#))
       (valAt [this# k# not-found#]
         (.nth this# k# not-found#))
       (assocN [this# k# ~'v]
         (when (< k# 0)
           (errors/throw-index-out-of-boundsf "negative index: %s" k#))
         (errors/check-idx k# ~(inc argc))
         (if (== k# ~argc)
           (imlist ~@argvec ~'v)
           (case k#
             ~@(->> (range argc)
                    (mapcat (fn [idx]
                              [idx `(imlist-meta
                                     ~'metadata
                                     ~@(concat
                                        (subvec argvec 0 idx)
                                        ['v]
                                        (subvec argvec (inc (long idx)) argc)))]))))))
       IObj
       (meta [this#] ~'metadata)
       (withMeta [this# newmeta#]
         (imlist-meta newmeta# ~@argvec))
       Object
       (hashCode [this#]
         (if (== 0 ~'hash-code)
           (set! ~'hash-code (unchecked-int (list-hashcode this#)))
           ~'hash-code))
       (equals [this# o#]
         (list-equals this# o#))
       (toString [this#]
         (dtype-pp/buffer->string this# "imlist")))))


(define-imlist 1)
(define-imlist 2)
(define-imlist 3)
(define-imlist 4)
(define-imlist 5)
(define-imlist 6)
(define-imlist 7)
(define-imlist 8)
(define-imlist 9)
(define-imlist 10)


(defn imlist-meta
  "Define an immutable list with exactly these elements.  This does so with 1 allocation
  of an object of fixed size.
  Returns a list with exactly the same values for hashCode and hasheq as a Clojure persistent
  vector."
  (^List [meta] (with-meta [] meta))
  (^List [meta arg] (ImList1. 0 0 meta arg))
  (^List [meta arg0 arg1] (ImList2. 0 0 meta arg0 arg1))
  (^List [meta arg0 arg1 arg3] (ImList3. 0 0 meta arg0 arg1 arg3))
  (^List [meta arg0 arg1 arg3 arg4] (ImList4. 0 0 meta arg0 arg1 arg3 arg4))
  (^List [meta arg0 arg1 arg3 arg4 arg5] (ImList5. 0 0 meta arg0 arg1 arg3 arg4 arg5))
  (^List [meta arg0 arg1 arg3 arg4 arg5
          arg6] (ImList6. 0 0 meta arg0 arg1 arg3 arg4 arg5 arg6))
  (^List [meta arg0 arg1 arg3 arg4 arg5
          arg6 arg7] (ImList7. 0 0 meta arg0 arg1 arg3 arg4 arg5 arg6 arg7))
  (^List [meta arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8] (ImList8. 0 0 meta arg0 arg1 arg3 arg4 arg5 arg6 arg7 arg8))
  (^List [meta arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8 arg9] (ImList9. 0 0 meta arg0 arg1 arg3 arg4 arg5 arg6 arg7 arg8 arg9))
  (^List [meta arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8 arg9 arg10] (ImList10. 0 0 meta arg0 arg1 arg3 arg4 arg5
                                                arg6 arg7 arg8 arg9 arg10))
  (^List [meta arg0 arg1 arg2 arg3 arg4
          arg5 arg6 arg7 arg8 arg9 & args]
   (with-meta
     (let [n-args (count args)
           obj-data (object-array (+ 10 n-args))]
       (ArrayHelpers/aset obj-data 0 arg0)
       (ArrayHelpers/aset obj-data 1 arg1)
       (ArrayHelpers/aset obj-data 2 arg2)
       (ArrayHelpers/aset obj-data 3 arg3)
       (ArrayHelpers/aset obj-data 4 arg4)
       (ArrayHelpers/aset obj-data 5 arg5)
       (ArrayHelpers/aset obj-data 6 arg6)
       (ArrayHelpers/aset obj-data 7 arg7)
       (ArrayHelpers/aset obj-data 8 arg8)
       (ArrayHelpers/aset obj-data 9 arg9)
       (loop [idx 10
              args args]
         (when args
           (ArrayHelpers/aset obj-data idx (RT/first args))
           (recur (unchecked-inc idx) (RT/next args))))
       (LazilyPersistentVector/create obj-data))
     meta)))


(defn imlist
  "Define an immutable list with exactly these elements.  This does so with 1 allocation
  of an object of fixed size.
  Returns a list with exactly the same values for hashCode and hasheq as a Clojure persistent
  vector."
  (^List [] [])
  (^List [arg] (ImList1. 0 0 nil arg))
  (^List [arg0 arg1] (ImList2. 0 0 nil arg0 arg1))
  (^List [arg0 arg1 arg3] (ImList3. 0 0 nil arg0 arg1 arg3))
  (^List [arg0 arg1 arg3 arg4] (ImList4. 0 0 nil arg0 arg1 arg3 arg4))
  (^List [arg0 arg1 arg3 arg4 arg5] (ImList5. 0 0 nil arg0 arg1 arg3 arg4 arg5))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6] (ImList6. 0 0 nil arg0 arg1 arg3 arg4 arg5 arg6))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7] (ImList7. 0 0 nil arg0 arg1 arg3 arg4 arg5 arg6 arg7))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8] (ImList8. 0 0 nil arg0 arg1 arg3 arg4 arg5 arg6 arg7 arg8))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8 arg9] (ImList9. 0 0 nil arg0 arg1 arg3 arg4 arg5 arg6 arg7 arg8 arg9))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8 arg9 arg10] (ImList10. 0 0 nil arg0 arg1 arg3 arg4 arg5
                                                arg6 arg7 arg8 arg9 arg10))
  (^List [arg0 arg1 arg2 arg3 arg4
          arg5 arg6 arg7 arg8 arg9 & args]
   (let [n-args (count args)
         obj-data (object-array (+ 10 n-args))]
     (ArrayHelpers/aset obj-data 0 arg0)
     (ArrayHelpers/aset obj-data 1 arg1)
     (ArrayHelpers/aset obj-data 2 arg2)
     (ArrayHelpers/aset obj-data 3 arg3)
     (ArrayHelpers/aset obj-data 4 arg4)
     (ArrayHelpers/aset obj-data 5 arg5)
     (ArrayHelpers/aset obj-data 6 arg6)
     (ArrayHelpers/aset obj-data 7 arg7)
     (ArrayHelpers/aset obj-data 8 arg8)
     (ArrayHelpers/aset obj-data 9 arg9)
     (loop [idx 10
            args args]
       (when args
         (ArrayHelpers/aset obj-data idx (RT/first args))
         (recur (unchecked-inc idx) (RT/next args))))
     (LazilyPersistentVector/create obj-data))))
