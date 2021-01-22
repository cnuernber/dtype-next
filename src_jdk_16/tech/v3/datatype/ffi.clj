(ns tech.v3.datatype.ffi
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [insn.core :as insn]
            [clojure.string :as s])
  (:import [jdk.incubator.foreign LibraryLookup CLinker FunctionDescriptor
            MemoryLayout LibraryLookup$Symbol]
           [jdk.incubator.foreign MemoryAddress]
           [java.lang.invoke MethodHandle MethodType]
           [java.nio.file Path Paths]
           [tech.v3.datatype NumericConversions]
           [tech.v3.datatype.ffi InvokeStub]
           [clojure.lang IFn RT ISeq Var]))


(set! *warn-on-reflection* true)

;;  https://openjdk.java.net/projects/jdk/16/
;;  https://download.java.net/java/early_access/jdk16/docs/api/jdk.incubator.foreign/jdk/incubator/foreign/CLinker.html
;;  https://openjdk.java.net/jeps/389
;;  https://github.com/cs-activeviam-simd/vector-profiling/blob/master/src/test/java/fr/centralesupelec/simd/VectorOffHeapTest.java


;; MethodHandle strlen = CLinker.getInstance().downcallHandle(
;;         LibraryLookup.ofDefault().lookup("strlen"),
;;         MethodType.methodType(long.class, MemoryAddress.class),
;;         FunctionDescriptor.of(C_LONG, C_POINTER)
;;     );

(defn memory-layout-array
  ^"[Ljdk.incubator.foreign.MemoryLayout;" [& args]
  (into-array MemoryLayout args))

(defn ->path
  ^Path [str-base & args]
  (Paths/get (str str-base) (into-array String args)))

(defn find-symbol
  (^LibraryLookup$Symbol [libname symbol-name]
   (if (instance? java.nio.file.Path libname)
     (-> (.lookup (LibraryLookup/ofPath libname) symbol-name)
         (.get))
     (-> (.lookup (LibraryLookup/ofLibrary libname) symbol-name)
         (.get))))
  (^LibraryLookup$Symbol [symbol-name]
   (-> (.lookup (LibraryLookup/ofDefault) (str symbol-name))
       (.get))))


(defn ->memory-address
  [item]
  (cond
    (nil? item)
    (MemoryAddress/ofLong 0)
    (string? item)
    (.address (CLinker/toCString item))
    :else
    (if-let [nbuf (dtype-base/as-native-buffer item)]
      (MemoryAddress/ofLong (.address nbuf))
      (errors/throwf "Item type %s is not convertible to memory address"
                     (type item)))))


(defn argtype->mem-layout-type
  [argtype]
  (case argtype
    :int8 CLinker/C_CHAR
    :int16 CLinker/C_SHORT
    :int32 CLinker/C_INT
    :int64 CLinker/C_LONG
    :float32 CLinker/C_FLOAT
    :float64 CLinker/C_DOUBLE
    :pointer? CLinker/C_POINTER
    :pointer CLinker/C_POINTER))


(defn sig->fdesc
  ^FunctionDescriptor [{:keys [rettype argtypes]}]
  (if (nil? rettype)
    (FunctionDescriptor/ofVoid (->> argtypes
                                    (map argtype->mem-layout-type)
                                    (apply memory-layout-array)))
    (FunctionDescriptor/of (argtype->mem-layout-type rettype)
                           (->> argtypes
                                (map argtype->mem-layout-type)
                                (apply memory-layout-array)))))

(defn argtype->cls
  ^Class [argtype]
  (case argtype
    :int8 Byte/TYPE
    :int16 Short/TYPE
    :int32 Integer/TYPE
    :int64 Long/TYPE
    :float32 Float/TYPE
    :float64 Double/TYPE
    :pointer MemoryAddress
    :string MemoryAddress))

(defn sig->method-type
  ^MethodType [{:keys [rettype argtypes]}]
  (let [^"[Ljava.lang.Class;" cls-ary (->> argtypes
                                           (map argtype->cls)
                                           (into-array Class))]
    (MethodType/methodType (argtype->cls rettype) cls-ary)))

(defn sig->cls-name
  [{:keys [rettype argtypes]}]
  (->> (concat [rettype] argtypes)
       (map (fn [argtype]
              (if-not (nil? argtype)
                (case argtype
                  :int8 "B"
                  :int16 "S"
                  :int32 "I"
                  :int64 "J"
                  :float32 "F"
                  :float64 "D"
                  "P")
                "V")))
       (s/join "_")))

(def constructor-code
  )

(defn argtype->insn-type
  [argtype]
  (if (nil? argtype)
    :void
    (case argtype
      :int8 :byte
      :int16 :short
      :int32 :int
      :int64 :long
      :float32 :float
      :float64 :double
      MemoryAddress)))

(defn emit-insn-for-sig
  [{:keys [rettype argtypes]}]
  (concat
   [[:aload 0]
    [:getfield :this "methodHandle" MethodHandle]]
   (->> argtypes
        (map-indexed vector)
        (mapcat (fn [[idx argtype]]
                  (let [arg-idx (inc (long idx))]
                    (concat
                     [[:aload arg-idx]]
                     (case argtype
                       :int8 [[:invokestatic NumericConversions "numberCast" [:object Number]]
                              [:invokestatic RT "uncheckedByteCast" [:object Byte/TYPE]]]
                       :int16 [[:invokestatic NumericConversions "numberCast" [:object Number]]
                               [:invokestatic RT "uncheckedShortCast" [:object Short/TYPE]]]
                       :int32 [[:invokestatic NumericConversions "numberCast" [:object Number]]
                               [:invokestatic RT "uncheckedIntCast" [:object Integer/TYPE]]]
                       :int64 [[:invokestatic NumericConversions "numberCast" [:object Number]]
                               [:invokestatic RT "uncheckedIntCast" [:object Long/TYPE]]]
                       :float32 [[:invokestatic NumericConversions "numberCast" [:object Number]]
                                 [:invokestatic RT "uncheckedFloatCast" [:object Float/TYPE]]]
                       :float64 [[:invokestatic NumericConversions "numberCast" [:object Number]]
                                 [:invokestatic RT "uncheckedDoubleCast" [:object Double/TYPE]]]
                       [[:aload 0]
                        [:getfield :this "asMemoryAddr" IFn]
                        ;;Swap the IFn 'this' ptr and the argument on the stack
                        [:swap]
                        [:invokeinterface IFn "invoke" [Object Object]]
                        [:checkcast MemoryAddress]]))))))
   [
    (concat [[:invokevirtual MethodHandle "invokeExact" (mapv argtype->insn-type (concat argtypes [rettype]))]]
            (case rettype
              :int8 [[:invokestatic Byte "valueOf" [:byte Byte]]
                     [:areturn]]
              :int16 [[:invokestatic Short "valueOf" [:short Short]]
                      [:areturn]]
              :int32 [[:invokestatic Integer "valueOf" [:int Integer]]
                      [:areturn]]
              :int64 [[:invokestatic Long "valueOf" [:long Long]]
                      [:areturn]]
              :float32 [[:invokestatic Float "valueOf" [:float Float]]
                        [:areturn]]
              :float64 [[:invokestatic Double "valueOf" [:double Double]]
                        [:areturn]]
              (if (nil? rettype)
                [[:return]]
                [[:areturn]])))]))


(defn emit-insn-apply-to
  [^long n-args]
  (concat
   [[:aload 0]
    [:dup]
    [:aload 1]]
   (->> (for [idx (range n-args)]
          [[:dup]
           [:invokeinterface ISeq "first" [Object]]
           [:swap]
           [:invokeinterface ISeq "next" [ISeq]]])
        (apply concat))
   [[:pop]
    [:invokevirtual :this "invoke" (vec (repeat (inc n-args) Object))]
    [:areturn]]))


(defn sig->class-def
  [{:keys [argtypes] :as sig}]
  (let [cls-name (sig->cls-name sig)]
    {:name (symbol (str "test.v3.datatype.ffi." cls-name))
     :interfaces [IFn]
     :fields [{:flags #{:public :final}
               :name "methodHandle"
               :type MethodHandle}
              {:flags #{:public :final}
               :name "asMemoryAddr"
               :type IFn}]
     :methods [{:flags #{:public}
                :name :init
                :desc [MethodHandle IFn :void]
                :emit [[:aload 0]
                       [:invokespecial :super :init [:void]]
                       [:aload 0]
                       [:aload 1]
                       [:checkcast MethodHandle]
                       [:putfield :this "methodHandle" MethodHandle]
                       [:aload 0]
                       [:aload 2]
                       [:checkcast IFn]
                       [:putfield :this "asMemoryAddr" IFn]
                       [:return]]}
               {:flags #{:public}
                :name :invoke
                :desc (vec (repeat (inc (count argtypes)) Object))
                :emit (emit-insn-for-sig sig)}
               {:flags #{:public}
                :name :applyTo
                :desc [ISeq Object]
                :emit (emit-insn-apply-to (count argtypes))}
               ]}))

(defn strlen
  [data]
  (let [linker (CLinker/getInstance)
        symbol (find-symbol "strlen")
        sig {:rettype :int64 :argtypes [:pointer]}
        jvm-sig (sig->method-type sig)
        c-sig (sig->fdesc sig)
        mh (.downcallHandle linker symbol jvm-sig c-sig)
        ms (CLinker/toCString (str data))]
    (InvokeStub/invokeExactJ mh (.address ms))))
