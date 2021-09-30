(ns primitive-math
  (:refer-clojure
    :exclude [* + - / < > <= >= == rem bit-or bit-and bit-xor bit-not bit-shift-left bit-shift-right unsigned-bit-shift-right byte short int float long double inc dec zero? min max true? false?])
  (:import
    [primitive_math Primitives]
    [java.nio ByteBuffer]))

;;;

(defmacro ^:private variadic-proxy
  "Creates left-associative variadic forms for any operator."
  ([name fn]
     `(variadic-proxy ~name ~fn ~(str "A primitive macro version of `" name "`")))
  ([name fn doc]
     `(variadic-proxy ~name ~fn ~doc identity))
  ([name fn doc single-arg-form]
     (let [x-sym (gensym "x")]
       `(defmacro ~name
          ~doc
          ([~x-sym]
             ~((eval single-arg-form) x-sym))
          ([x# y#]
             (list '~fn x# y#))
          ([x# y# ~'& rest#]
             (list* '~name (list '~name x# y#) rest#))))))

(defmacro ^:private variadic-predicate-proxy
  "Turns variadic predicates into multiple pair-wise comparisons."
  ([name fn]
     `(variadic-predicate-proxy ~name ~fn ~(str "A primitive macro version of `" name "`")))
  ([name fn doc]
     `(variadic-predicate-proxy ~name ~fn ~doc (constantly true)))
  ([name fn doc single-arg-form]
     (let [x-sym (gensym "x")]
       `(defmacro ~name
          ~doc
          ([~x-sym]
             ~((eval single-arg-form) x-sym))
          ([x# y#]
             (list '~fn x# y#))
          ([x# y# ~'& rest#]
             (list 'primitive_math.Primitives/and (list '~name x# y#) (list* '~name y# rest#)))))))

(variadic-proxy +             primitive_math.Primitives/add)
(variadic-proxy -             primitive_math.Primitives/subtract "A primitive macro version of `-`" (fn [x] `(list 'primitive_math.Primitives/negate ~x)))
(variadic-proxy *             primitive_math.Primitives/multiply)
(variadic-proxy /             primitive_math.Primitives/divide)
(variadic-proxy div           primitive_math.Primitives/divide)
(variadic-proxy bit-and       primitive_math.Primitives/bitAnd)
(variadic-proxy bit-or        primitive_math.Primitives/bitOr)
(variadic-proxy bit-xor       primitive_math.Primitives/bitXor)
(variadic-proxy bool-and      primitive_math.Primitives/and)
(variadic-proxy bool-or       primitive_math.Primitives/or)
(variadic-proxy bool-xor      primitive_math.Primitives/xor)
(variadic-proxy min           primitive_math.Primitives/min)
(variadic-proxy max           primitive_math.Primitives/max)

(variadic-predicate-proxy >   primitive_math.Primitives/gt)
(variadic-predicate-proxy <   primitive_math.Primitives/lt)
(variadic-predicate-proxy <=  primitive_math.Primitives/lte)
(variadic-predicate-proxy >=  primitive_math.Primitives/gte)
(variadic-predicate-proxy ==  primitive_math.Primitives/eq)
(variadic-predicate-proxy not==  primitive_math.Primitives/neq "A primitive macro complement of `==`")

(defmacro inc
  "A primitive macro version of `inc`."
  [x]
  `(Primitives/inc ~x))

(defmacro dec
  "A primitive macro version of `dec`."
  [x]
  `(Primitives/dec ~x))

(defmacro rem
  "A primitive macro version of `rem`."
  [n div]
  `(Primitives/rem ~n ~div))

(defmacro zero?
  "A primitive macro version of `zero?`."
  [x]
  `(Primitives/isZero ~x))

(defmacro bool-not
  "A primitive macro version of `not`."
  [x]
  `(Primitives/not ~x))

(defmacro bit-not
  "A primitive macro version of `bit-not`."
  [x]
  `(Primitives/bitNot ~x))

(defmacro true?
  "A primitive macro version of `true?`."
  [x]
  `(Primitives/isTrue ~x))

(defmacro false?
  "A primitive macro version of `false?`."
  [x]
  `(Primitives/isFalse ~x))

(defmacro bit-shift-left
  "A primitive macro version of `bit-shift-left`."
  [n bits]
  `(Primitives/shiftLeft ~n ~bits))

(defmacro bit-shift-right
  "A primitive macro version of `bit-shift-right`."
  [n bits]
  `(Primitives/shiftRight ~n ~bits))

;; this was the original name, which doesn't match the Clojure name and is kept
;; around for legacy purposes
(defmacro ^:no-doc bit-unsigned-shift-right
  [n bits]
  `(Primitives/unsignedShiftRight ~n ~bits))

(defmacro unsigned-bit-shift-right
  "A primitive macro which performs an unsigned right bit-shift."
  [n bits]
  `(Primitives/unsignedShiftRight ~n ~bits))

(defmacro <<
  "An alias for `bit-shift-left`."
  [n bits]
  `(Primitives/shiftLeft ~n ~bits))

(defmacro >>
  "An alias for `bit-shift-right`."
  [n bits]
  `(Primitives/shiftRight ~n ~bits))

(defmacro >>>
  "An alias for `bit-unsigned-shift-right`."
  [n bits]
  `(Primitives/unsignedShiftRight ~n ~bits))

;;;

(def ^:private vars-to-exclude
  '[* + - / < > <= >= == rem bit-or bit-and bit-xor bit-not bit-shift-left bit-shift-right byte short int float long double inc dec zero? true? false? min max])

(defn- using-primitive-operators? []
  (= #'primitive-math/+ (resolve '+)))

(defonce ^:private hijacked? (atom false))

(defn- ns-wrapper
  "Makes sure that if a namespace that is using primitive operators is reloaded, it will automatically
   exclude the shadowed operators in `clojure.core`."
  [f]
  (fn [& x]
    (if-not (using-primitive-operators?)
      (apply f x)
      (let [refer-clojure (->> x
                            (filter #(and (sequential? %) (= :refer-clojure (first %))))
                            first)
            refer-clojure-clauses (update-in
                                    (apply hash-map (rest refer-clojure))
                                    [:exclude]
                                    #(concat % vars-to-exclude))]
        (apply f
          (concat
            (remove #{refer-clojure} x)
            [(list* :refer-clojure (apply concat refer-clojure-clauses))]))))))

(defn use-primitive-operators
  "Replaces Clojure's arithmetic and number coercion functions with primitive equivalents.  These are
   defined as macros, so they cannot be used as higher-order functions.  This is an idempotent operation.."
  []
  (when-not @hijacked?
    (reset! hijacked? true)
    (alter-var-root #'clojure.core/ns ns-wrapper))
  (when-not (using-primitive-operators?)
    (doseq [v vars-to-exclude]
      (ns-unmap *ns* v))
    (require (vector 'primitive-math :refer vars-to-exclude))))

(defn unuse-primitive-operators
  "Undoes the work of `use-primitive-operators`.  This is idempotent."
  []
  (doseq [v vars-to-exclude]
    (ns-unmap *ns* v))
  (refer 'clojure.core))

;;;

(defn byte
  "Truncates a number to a byte, will not check for overflow."
  {:inline (fn [x] `(primitive_math.Primitives/toByte ~x))}
  ^long [^long x]
  (unchecked-long (Primitives/toByte x)))

(defn short
  "Truncates a number to a short, will not check for overflow."
  {:inline (fn [x] `(primitive_math.Primitives/toShort ~x))}
  ^long [^long x]
  (unchecked-long (Primitives/toShort x)))

(defn int
  "Truncates a number to an int, will not check for overflow."
  {:inline (fn [x] `(primitive_math.Primitives/toInteger ~x))}
  ^long [^long x]
  (unchecked-long (Primitives/toInteger x)))

(defn float
  "Truncates a number to a float, will not check for overflow."
    {:inline (fn [x] `(primitive_math.Primitives/toFloat ~x))}
  ^double [^double x]
  (unchecked-double (Primitives/toFloat x)))

(defn long
  "Converts a number to a long."
  {:inline (fn [x] `(unchecked-long ~x))}
  ^long [x]
  (unchecked-long x))

(defn double
  "Converts a number to a double."
  {:inline (fn [x] `(unchecked-double ~x))}
  ^double [x]
  (unchecked-double x))

(defn byte->ubyte
  "Converts a byte to an unsigned byte."
  {:inline (fn [x] `(->> ~x long (bit-and 0xFF) short))}
  ^long [^long x]
  (long (short (bit-and x 0xFF))))

(defn ubyte->byte
  "Converts an unsigned byte to a byte."
  {:inline (fn [x] `(byte (long ~x)))}
  ^long [^long x]
  (long (byte x)))

(defn short->ushort
  "Converts a short to an unsigned short."
  {:inline (fn [x] `(->> ~x long (bit-and 0xFFFF) int))}
  ^long [^long x]
  (long (int (bit-and 0xFFFF x))))

(defn ushort->short
  "Converts an unsigned short to a short."
  {:inline (fn [x] `(short (long ~x)))}
  ^long [^long x]
  (long (short x)))

(defn int->uint
  "Converts an integer to an unsigned integer."
  {:inline (fn [x] `(->> ~x long (bit-and 0xFFFFFFFF)))}
  ^long [^long x]
  (long (bit-and 0xFFFFFFFF x)))

(defn uint->int
  "Converts an unsigned integer to an integer."
  {:inline (fn [x] `(int (long ~x)))}
  ^long [^long x]
  (long (int x)))

(defn long->ulong
  "Converts a long to an unsigned long."
  [^long x]
  (BigInteger. 1
    (-> (ByteBuffer/allocate 8) (.putLong x) .array)))

(defn ^long ulong->long
  "Converts an unsigned long to a long."
  ^long [x]
  (.longValue ^clojure.lang.BigInt (bigint x)))

(defn float->int
  "Converts a float to an integer."
  {:inline (fn [x] `(Float/floatToRawIntBits (float ~x)))}
  ^long [^double x]
  (long (Float/floatToRawIntBits x)))

(defn int->float
  "Converts an integer to a float."
  {:inline (fn [x] `(Float/intBitsToFloat (int ~x)))}
  ^double [^long x]
  (double (Float/intBitsToFloat x)))

(defn double->long
  "Converts a double to a long."
  {:inline (fn [x] `(Double/doubleToRawLongBits ~x))}
  ^long [^double x]
  (long (Double/doubleToRawLongBits x)))

(defn long->double
  "Converts a long to a double."
  {:inline (fn [x] `(Double/longBitsToDouble ~x))}
  ^double [^long x]
  (double (Double/longBitsToDouble x)))

(defn reverse-short
  "Inverts the endianness of a short."
  {:inline (fn [x] `(Primitives/reverseShort ~x))}
  ^long [^long x]
  (->> x Primitives/reverseShort long))

(defn reverse-int
  "Inverts the endianness of an int."
  {:inline (fn [x] `(Primitives/reverseInteger ~x))}
  ^long [^long x]
  (->> x Primitives/reverseInteger long))

(defn reverse-long
  "Inverts the endianness of a long."
  {:inline (fn [x] `(Primitives/reverseLong ~x))}
  ^long [^long x]
  (Primitives/reverseLong x))

(defn reverse-float
  "Inverts the endianness of a float."
  {:inline (fn [x] `(-> ~x float->int reverse-int int->float))}
  ^double [^double x]
  (-> x float->int reverse-int int->float))

(defn reverse-double
  "Inverts the endianness of a double."
  {:inline (fn [x] `(-> ~x double->long reverse-long long->double))}
  ^double [^double x]
  (-> x double->long reverse-long long->double))
