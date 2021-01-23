(ns tech.v3.datatype.ffi
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [insn.core :as insn]
            [clojure.string :as s]
            [clojure.java.io :as io])
  (:import [jdk.incubator.foreign LibraryLookup CLinker FunctionDescriptor
            MemoryLayout LibraryLookup$Symbol]
           [jdk.incubator.foreign MemoryAddress Addressable]
           [java.lang.invoke MethodHandle MethodType]
           [java.nio.file Path Paths]
           [java.lang.reflect Constructor]
           [tech.v3.datatype NumericConversions]
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


(defprotocol PToMemoryAddress
  (convertible-to-memory-address? [item]
    "Is this object convertible to a MemoryAddress?")
  (->memory-address ^MemoryAddress [item]
    "Conversion to a MemoryAddress"))


(extend-type Object
  PToMemoryAddress
  (is-memory-address-convertible? [item]
    (dtype-proto/convertible-to-native-buffer? item))
  (->memory-address [item]
    (let [nbuf (dtype-base/->native-buffer item)]
      (MemoryAddress/ofLong (.address nbuf)))))


(defn ptr-convertible?
  "Is this object pointer convertible via the PToPtr protocol."
  [item]
  (when item (convertible-to-memory-address? item)))


(defn as-ptr
  "Convert this object to a jna Pointer MEmoryAddress that points to 0 if not possible."
  ^MemoryAddress [item]
  (if (and item (ptr-convertible? item))
    (->memory-address item)
    (MemoryAddress/ofLong 0)))


(defn ensure-ptr
  "Ensure this is a non-nil ptr"
  [item]
  (when-not (and item (ptr-convertible? item))
    (errors/throwf "Item is not pointer convertible: %s" item))
  (as-ptr item))


(extend-protocol PToMemoryAddress
  MemoryAddress
  (is-memory-address-convertible? [item] true)
  (->ptr-backing-store [item] item)
  Addressable
  (is-memory-address-convertible? [item] true)
  (->memory-address [item] (.address item))
  String
  (is-memory-address-convertible? [item] true)
  (->memory-address [item] (.address (CLinker/toCString (str item)))))

(defn ->path
  ^Path [str-base & args]
  (Paths/get (str str-base) (into-array String args)))

(defn find-symbol
  (^LibraryLookup$Symbol [libname symbol-name]
   (let [symbol-name (cond
                       (symbol? symbol-name)
                       (name symbol-name)
                       (keyword? symbol-name)
                       (name symbol-name)
                       :else
                       (str symbol-name))]
     (cond
       (instance? java.nio.file.Path libname)
       (-> (.lookup (LibraryLookup/ofPath libname) symbol-name)
           (.get))
       (string? libname)
       (-> (.lookup (LibraryLookup/ofLibrary libname) symbol-name)
           (.get))
       (nil? libname)
       (-> (.lookup (LibraryLookup/ofDefault) symbol-name)
           (.get))
       :else
       (errors/throwf "Inrecognized libname type: %s"))))
  (^LibraryLookup$Symbol [symbol-name]
   (find-symbol nil symbol-name)))


(defn memory-layout-array
  ^"[Ljdk.incubator.foreign.MemoryLayout;" [& args]
  (into-array MemoryLayout args))


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
                       :int8 [[:invokestatic NumericConversions "numberCast" [Object Number]]
                              [:invokestatic RT "uncheckedByteCast" [Object Byte/TYPE]]]
                       :int16 [[:invokestatic NumericConversions "numberCast" [Object Number]]
                               [:invokestatic RT "uncheckedShortCast" [Object Short/TYPE]]]
                       :int32 [[:invokestatic NumericConversions "numberCast" [Object Number]]
                               [:invokestatic RT "uncheckedIntCast" [Object Integer/TYPE]]]
                       :int64 [[:invokestatic NumericConversions "numberCast" [Object Number]]
                               [:invokestatic RT "uncheckedLongCast" [Object Long/TYPE]]]
                       :float32 [[:invokestatic NumericConversions "numberCast" [Object Number]]
                                 [:invokestatic RT "uncheckedFloatCast" [Object Float/TYPE]]]
                       :float64 [[:invokestatic NumericConversions "numberCast" [Object Number]]
                                 [:invokestatic RT "uncheckedDoubleCast" [Object Double/TYPE]]]
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
              (if (or (nil? rettype)
                      (= :void rettype))
                [[:return]]
                [[:areturn]])))]))


(defn sig->full-cls-symbol
  [sig]
  (symbol (str "tech.v3.datatype.ffi.Invoker_" (sig->cls-name sig))))


(defn sig->class-def
  [{:keys [argtypes] :as sig}]
  {:name (sig->full-cls-symbol sig)
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
              :emit (emit-insn-for-sig sig)}]})


(defn find-function
  "Find the symbol and return an IFn implementation wrapping efficient access
  to the function."
  [libname symbol-name rettype argtypes & [invoker-constructor]]
  (let [sym (find-symbol libname symbol-name)
        sig {:rettype rettype
             :argtypes argtypes}
        fndesc (sig->fdesc sig)
        methoddesc (sig->method-type sig)
        linker (CLinker/getInstance)
        method-handle (.downcallHandle linker sym methoddesc fndesc)
        invoker-constructor
        (or invoker-constructor
            (fn [sig mh ma-fn]
              (let [fn-invoke-cls (insn/define (sig->class-def sig))
                    ^Constructor fn-invoke-constructor
                    (first (.getDeclaredConstructors fn-invoke-cls))]
                (.newInstance fn-invoke-constructor (object-array [mh ma-fn])))))]
    (invoker-constructor sig method-handle ->memory-address)))


(defn define-and-save
  "currently broken"
  [sig]
  (let [class-def (sig->class-def sig)
        cls-name (name (:name class-def))
        lastdot (.lastIndexOf cls-name ".")
        package (.substring cls-name 0 lastdot)
        cls-name (.substring cls-name (inc lastdot))
        output-path (str "target/classes/"
                         (.replace package "." "/")
                         "/" cls-name ".class")]
    (io/make-parents output-path)
    (insn/write (insn/visit class-def) "target/classes")
    output-path))



(defmacro def-ffi-fn
  [libname symbol-name docstr rettype & arglist]
  (let [fn-args (mapv second arglist)
        fn-argtypes (mapv first arglist)
        sym-name (name symbol-name)]
    `(let [~'fn-obj* (delay (find-function ~libname ~sym-name
                                           ~rettype ~fn-argtypes))]
       (defn ~symbol-name
         ~docstr
         ~fn-args
         ~(if (== 0 (count arglist))
            `((deref ~'fn-obj*))
            `((deref ~'fn-obj*) ~@fn-args))))))

(def arch64-set #{"x86_64" "amd64"})


(defn size-t-size
  ^long []
  (let [arch (.toLowerCase (System/getProperty "os.arch"))]
    (if (arch64-set arch)
      8
      4)))


(defmacro size-t-compile-time-switch
  "Run either int32 based code or int64 based code depending
   on the runtime size of size-t"
  [int-body long-body]
  (case (size-t-size)
    4 `~int-body
    8 `~long-body))


(defn size-t-type
  []
  (if (= (size-t-size) 8)
    :int64
    :int32))

(defn ptr-int-type
  []
  (size-t-type))
