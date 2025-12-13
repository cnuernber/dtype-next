(ns tech.v3.datatype.gen-java)

(defmacro unsigned-long-hasheq
  [vv]
  `(let [~(with-meta 'vv {:tag 'clojure.lang.IHashEq}) ~vv]
     (Integer/toUnsignedLong (.hasheq ~'vv))))

(defmacro masked-hasheq
  [mask obj]
  `(bit-and (unsigned-long-hasheq ~obj) ~mask))

(defn bits->mask ^long [^long bits] (dec (bit-shift-left 1 bits)))

(defn hasheq-collision
  [^long shift items]
  (let [hs (java.util.HashSet.)
        mask (bits->mask shift)]
    (reduce (fn [rv kw]
              (if (not (.add hs (masked-hasheq mask kw)))
                (reduced true)
                false))
            false
            items)))

(defn min-hasheq-collision-bits
  ^long [objs]
  (long (or (->> (range 1 32)
                 (remove #(hasheq-collision % objs))
                 (first))
            31)))

(defn generate-keyword-set-inclusion-tests
  [out-dir classname kw-set-map]
  (with-open [of (-> (java.io.FileOutputStream. (str out-dir classname ".java"))
                     (java.io.BufferedOutputStream.)
                     (java.io.OutputStreamWriter.))]
    (.write of "package tech.v3.datatype;
import clojure.lang.RT;
import clojure.lang.Keyword;
import clojure.lang.IHashEq;
")
    (.write of (str "public class " classname " {\n"))
    (let [all-kws (vec (sort (distinct (apply concat (vals kw-set-map)))))]
      (run!
       (fn [kw]
         (.write of (str "  public static final Keyword k_" (name kw)
                         " = RT.keyword(null, \"" (name kw) "\");\n")))
       all-kws))
    (run! (fn [[test-name keywords]]
            (let [keywords (distinct keywords)
                  min-bits (min-hasheq-collision-bits keywords)
                  mask (bits->mask min-bits)
                  keywords (sort-by #(masked-hasheq mask %) keywords)]
              (.write of (str "  public static boolean " test-name "(final Object obj) {\n"))
              (.write of "    if(!(obj instanceof IHashEq)) return false;\n")
              (.write of (str "    final long hv = Integer.toUnsignedLong(((IHashEq)obj).hasheq()) & " mask ";
    switch((int)hv) {\n"))
              (run! (fn [kw]
                      (.write of (str "    case " (masked-hasheq mask kw) ": return obj == k_" (name kw) ";\n")))
                    keywords)
              (.write of "    default: return false;\n    }\n")
              (.write of "  }\n")))
          kw-set-map)
    (.write of "}\n")))

(comment
  (generate-keyword-set-inclusion-tests 
   "java/tech/v3/datatype/" 
   "KeywordSets"
   {"isBaseType" [:int8 :int16 :char :int32 :int64 :float32 :float64 :boolean :object]
    "isBaseNumericType" [:int8 :int16 :int32 :int64 :float32 :float64]
    "isPrimitiveType" [:int8 :int16 :int32 :int64 :float32 :float64 :boolean]
    "isKnownType" [:int8 :uint8 :int16 :uint16 :int32 :uint32 :int64 :uint64 :char
                   :float32 :float64 :boolean :object :string :keyword :uuid]
    "isIntegerType" [:int8 :uint8 :int16 :uint16 :int32 :uint32 :int64 :uint64]
    "isUnsignedIntegerType" [:uint8 :uint16 :uint32 :uint64]
    "isSignedIntegerType" [:int8 :int16 :int32 :int64]
    "isFloatType" [:float32 :float64]})

  :-)
