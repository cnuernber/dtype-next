(ns tech.v3.datatype.double-ops
  (:require [com.github.ztellman.primitive-math :as pmath]))


(defmacro SIGNIFICAND-BITS
  []
  `52)

(defmacro SIGNIFICAND-MASK [] `0x000fffffffffffff)

(defmacro IMPLICIT-BIT [] `(pmath/+ (SIGNIFICAND-MASK) 1))

(defmacro is-finite?
  [x]
  `(<= (Math/getExponent ~x)
       Double/MAX_EXPONENT))

(defn get-significand
  ^long [^double d]
  (when-not (is-finite? d)
    (throw (Exception. "not a normal value")))
  (let [exponent (Math/getExponent d)
        bits (Double/doubleToRawLongBits d)
        bits (bit-and bits (SIGNIFICAND-MASK))]
    (if (== exponent (- Double/MIN_EXPONENT 1))
      (bit-shift-left bits 1)
      (bit-or bits (IMPLICIT-BIT)))))


;; https://github.com/google/guava/blob/master/guava/src/com/google/common/math/DoubleMath.java#L279-L290
(defmacro is-mathematical-integer?
  [x]
  `(let [x# ~x]
    (boolean
     (and (is-finite? x#)
          (or (pmath/== x# 0.0)
              (pmath/<= (- (SIGNIFICAND-BITS)
                           (Long/numberOfTrailingZeros (get-significand x#)))
                        (Math/getExponent x#)))))))
