(ns tech.v3.datatype.ffi.mmodel
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.base :as ffi-base]
            [insn.core :as insn]
            [clojure.string :as s])
  (:import [jdk.incubator.foreign LibraryLookup CLinker FunctionDescriptor
            MemoryLayout LibraryLookup$Symbol]
           [jdk.incubator.foreign MemoryAddress Addressable MemoryLayout]
           [java.lang.invoke MethodHandle MethodType]
           [java.nio.file Path Paths]
           [java.lang.reflect Constructor]
           [tech.v3.datatype NumericConversions ClojureHelper]
           [tech.v3.datatype.ffi Pointer]
           [clojure.lang IFn RT ISeq Var Keyword IDeref]))


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


(defn ptr-value
  ^MemoryAddress [item]
  (MemoryAddress/ofLong (ffi/ptr-value item)))


(defn ptr-value?
  ^MemoryAddress [item]
  (MemoryAddress/ofLong (ffi/ptr-value? item)))


(extend-protocol ffi/PToPointer
  MemoryAddress
  (is-convertible-to-pointer? [item] true)
  (->pointer [item] (Pointer. (.toRawLongValue item)))
  Addressable
  (is-convertible-to-pointer? [item] true)
  (->pointer [item] (Pointer. (-> (.address item)
                                  (.toRawLongValue)))))


(defn ->path
  ^Path [str-base & args]
  (Paths/get (str str-base) (into-array String args)))


(defn load-library
  ^LibraryLookup [libname]
  (cond
    (instance? LibraryLookup libname)
    libname
    (instance? Path libname)
    (LibraryLookup/ofPath libname)
    (string? libname)
    (LibraryLookup/ofLibrary libname)
    (nil? libname)
    (LibraryLookup/ofDefault)
    :else
    (errors/throwf "Unrecognized libname type %s" (type libname))))


(defn find-symbol
  (^LibraryLookup$Symbol [libname symbol-name]
   (let [symbol-name (cond
                       (symbol? symbol-name)
                       (name symbol-name)
                       (keyword? symbol-name)
                       (name symbol-name)
                       :else
                       (str symbol-name))]
     (-> (load-library libname)
         (.lookup symbol-name)
         (.get))))
  (^LibraryLookup$Symbol [symbol-name]
   (find-symbol nil symbol-name)))


(defn memory-layout-array
  ^"[Ljdk.incubator.foreign.MemoryLayout;" [& args]
  (into-array MemoryLayout args))


(defn argtype->mem-layout-type
  [argtype]
  (case (ffi/lower-type argtype)
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
  (if (or (= :void rettype)
          (nil? rettype))
    (FunctionDescriptor/ofVoid (->> argtypes
                                    (map argtype->mem-layout-type)
                                    (apply memory-layout-array)))
    (FunctionDescriptor/of (argtype->mem-layout-type rettype)
                           (->> argtypes
                                (map argtype->mem-layout-type)
                                (apply memory-layout-array)))))


(defn argtype->cls
  ^Class [argtype]
  (case (ffi/lower-type argtype)
    :int8 Byte/TYPE
    :int16 Short/TYPE
    :int32 Integer/TYPE
    :int64 Long/TYPE
    :float32 Float/TYPE
    :float64 Double/TYPE
    :pointer MemoryAddress
    :pointer? MemoryAddress
    :string MemoryAddress
    :void Void/TYPE))


(defn sig->method-type
  ^MethodType [{:keys [rettype argtypes]}]
  (let [^"[Ljava.lang.Class;" cls-ary (->> argtypes
                                           (map argtype->cls)
                                           (into-array Class))]
    (MethodType/methodType (argtype->cls rettype) cls-ary)))


(defn library-sym->method-handle
  ^MethodHandle [library symbol-name rettype & argtypes]
  (let [sym (find-symbol library symbol-name)
        sig {:rettype rettype
             :argtypes argtypes}
        fndesc (sig->fdesc sig)
        methoddesc (sig->method-type sig)
        linker (CLinker/getInstance)]
    (.downcallHandle linker sym methoddesc fndesc)))


(defn emit-lib-constructor
  [fn-defs]
  (->>
   (concat
    [[:aload 0]
     [:invokespecial :super :init [:void]]]
    (ffi-base/find-ptr-ptrq "tech.v3.datatype.ffi.mmodel")
    [[:ldc "tech.v3.datatype.ffi.mmodel"]
     [:ldc "library-sym->method-handle"]
     [:invokestatic ClojureHelper "findFn" [String String IFn]]
     ;;1 is the string constructor argument
     ;;2 is the method handle resolution system
     [:astore 2]]
    ;;Load all the method handles.
    (mapcat
     (fn [[fn-name {:keys [rettype argtypes]}]]
       (let [hdl-name (str (name fn-name) "_hdl")]
         (concat
          [[:aload 0] ;;this-ptr
           [:aload 2] ;;handle resolution fn
           [:aload 1] ;;libname
           [:ldc (name fn-name)]
           [:ldc (name rettype)]
           [:invokestatic Keyword "intern" [String Keyword]]]
          (mapcat (fn [argtype]
                    [[:ldc (name argtype)]
                     [:invokestatic Keyword "intern" [String Keyword]]])
                  argtypes)
          [[:invokeinterface IFn "invoke"
            (concat [Object Object Object] (repeat (inc (count argtypes)) Object))]
           [:checkcast MethodHandle]
           [:putfield :this hdl-name MethodHandle]])))
     fn-defs)
    [[:aload 0]
    [:dup]
    [:invokevirtual :this "buildFnMap" [Object]]
    [:putfield :this "fnMap" Object]
    [:return]])
   (vec)))


(def ptr-cast (ffi-base/make-ptr-cast "asPointer" MemoryAddress))
(def ptr?-cast (ffi-base/make-ptr-cast "asPointerQ" MemoryAddress))
(def ptr-return (ffi-base/ptr-return
                 [[:invokeinterface MemoryAddress "toRawLongValue" [:long]]]))


(defn emit-fn-def
  [hdl-name rettype argtypes]
  (->> (concat
        [[:aload 0]
         [:getfield :this hdl-name MethodHandle]]
        (ffi-base/load-ffi-args ptr-cast ptr?-cast argtypes)
        [[:invokevirtual MethodHandle "invokeExact"
          (concat (map (partial ffi-base/argtype->insn
                                MemoryAddress :ptr-as-platform) argtypes)
                  [(ffi-base/argtype->insn MemoryAddress :ptr-as-platform
                                           rettype)])]]
        (ffi-base/ffi-call-return ptr-return rettype))
       (vec)))


(defn define-mmodel-library
  [classname fn-defs]
  [{:name classname
    :flags #{:public}
    :interfaces [IDeref]
    :fields (->> (concat
                  [{:name "asPointer"
                    :type IFn
                    :flags #{:public :final}}
                   {:name "asPointerQ"
                    :type IFn
                    :flags #{:public :final}}
                   {:name "fnMap"
                    :type Object
                    :flags #{:public :final}}]
                  (map (fn [[fn-name _fn-args]]
                         {:name (str (name fn-name) "_hdl")
                          :type MethodHandle
                          :flags #{:public :final}})
                       fn-defs))
                 (vec))
    :methods
    (->> (concat
          [{:name :init
            :flags #{:public}
            :desc [String :void]
            :emit (emit-lib-constructor fn-defs)}
           (ffi-base/emit-library-fn-map classname fn-defs)
           {:name :deref
            :desc [Object]
            :emit [[:aload 0]
                   [:getfield :this "fnMap" Object]
                   [:areturn]]}]
          (map
           (fn [[fn-name fn-data]]
             (let [hdl-name (str (name fn-name) "_hdl")
                   {:keys [rettype argtypes]} fn-data]
               {:name fn-name
                :flags #{:public}
                :desc (concat (map (partial ffi-base/argtype->insn
                                            MemoryAddress :ptr-as-obj)
                                   argtypes)
                              [(ffi-base/argtype->insn MemoryAddress :ptr-as-ptr
                                                       rettype)])
                :emit (emit-fn-def hdl-name rettype argtypes)}))
           fn-defs))
         (vec))}])


(defn define-library
  [fn-defs & [{:keys [classname]
               :or {classname (str "tech.v3.datatype.ffi.mmodel." (name (gensym)))}}]]
    (ffi-base/define-library fn-defs classname define-mmodel-library))


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library})
