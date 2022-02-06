(ns tech.v3.datatype.imlist
  "An extremely fast to construct immutable list type with compatible equiv/eq semantics
  to clojure.lang.APersistentVector."
  (:require [tech.v3.datatype.pprint :as dtype-pp])
  (:import [tech.v3.datatype ObjectReader]
           [clojure.lang Util Murmur3 IHashEq IPersistentCollection
            IPersistentVector IteratorSeq]
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
    `(deftype ~tname ~(apply vector hcsym hesym argvec)
       ObjectReader
       (lsize [this#] 1)
       (readObject [this# idx#]
         (let [idx# (long (if (< idx# 0)
                            (+ idx# ~argc)
                            idx#))]
           (case idx#
             ~@(->> (range argc)
                    (mapcat (fn [argidx]
                              [argidx (symbol (str "arg" argidx))])))
             (throw (Exception. (str "Index out of range: " idx#))))))
       IHashEq
       (hasheq [this#]
         (if (== 0 ~'hash-eq)
           (set! ~'hash-eq (unchecked-int (list-hasheq this#)))
           ~'hash-eq))
       IPersistentCollection
       (count [this#] 1)
       (cons [this# val#] (cons ~argvec val#))
       (empty [this#] (vector))
       (equiv [this# o#] (boolean (list-equiv this# o#)))
       (seq [this#] (IteratorSeq/create (.iterator this#)))
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


(defn imlist
  "Define an immutable list with exactly these elements.  This does so with 1 allocation
  of an object of fixed size.
  Returns a list with exactly the same values for hashCode and hasheq as a Clojure persistent
  vector."
  (^List [] [])
  (^List [arg] (ImList1. 0 0 arg))
  (^List [arg0 arg1] (ImList2. 0 0 arg0 arg1))
  (^List [arg0 arg1 arg3] (ImList3. 0 0 arg0 arg1 arg3))
  (^List [arg0 arg1 arg3 arg4] (ImList4. 0 0 arg0 arg1 arg3 arg4))
  (^List [arg0 arg1 arg3 arg4 arg5] (ImList5. 0 0 arg0 arg1 arg3 arg4 arg5))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6] (ImList6. 0 0 arg0 arg1 arg3 arg4 arg5 arg6))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7] (ImList7. 0 0 arg0 arg1 arg3 arg4 arg5 arg6 arg7))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8] (ImList8. 0 0 arg0 arg1 arg3 arg4 arg5 arg6 arg7 arg8))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8 arg9] (ImList9. 0 0 arg0 arg1 arg3 arg4 arg5 arg6 arg7 arg8 arg9))
  (^List [arg0 arg1 arg3 arg4 arg5
          arg6 arg7 arg8 arg9 arg10] (ImList10. 0 0 arg0 arg1 arg3 arg4 arg5
                                               arg6 arg7 arg8 arg9 arg10)))
