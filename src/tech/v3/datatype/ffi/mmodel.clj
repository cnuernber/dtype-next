(ns tech.v3.datatype.ffi.mmodel
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.base :as ffi-base])
  (:import [jdk.incubator.foreign LibraryLookup CLinker FunctionDescriptor
            MemoryLayout LibraryLookup$Symbol]
           [jdk.incubator.foreign MemoryAddress Addressable MemoryLayout]
           [java.lang.invoke MethodHandle MethodType MethodHandles]
           [java.nio.file Path Paths]
           [java.lang.reflect Constructor]
           [tech.v3.datatype NumericConversions ClojureHelper]
           [tech.v3.datatype.ffi Pointer Library]
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
  (MemoryAddress/ofLong (ffi-base/ptr-value item)))


(defn ptr-value?
  ^MemoryAddress [item]
  (MemoryAddress/ofLong (ffi-base/ptr-value? item)))


(extend-protocol ffi/PToPointer
  MemoryAddress
  (convertible-to-pointer? [item] true)
  (->pointer [item] (Pointer. (.toRawLongValue item)
                              {:src-ptr item}))
  Addressable
  (convertible-to-pointer? [item] true)
  (->pointer [item] (Pointer. (-> (.address item)
                                  (.toRawLongValue))
                              {:src-ptr item})))


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
    (let [libname (str libname)]
      (if (or (.contains libname "/")
              (.contains libname "\\"))
        (LibraryLookup/ofPath (Paths/get libname (into-array String [])))
        (LibraryLookup/ofLibrary libname)))
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
    :void Void/TYPE
    (do
      (errors/when-not-errorf
       (instance? Class argtype)
       "argtype (%s) must be instance of class" argtype)
      argtype)))


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
    [[:aload 0]
     [:ldc "tech.v3.datatype.ffi.mmodel"]
     [:ldc "load-library"]
     [:invokestatic ClojureHelper "findFn" [String String IFn]]
     [:aload 1]
     [:invokeinterface IFn "invoke" [Object Object]]
     [:checkcast LibraryLookup]
     [:putfield :this "libraryImpl" LibraryLookup]
     [:ldc "tech.v3.datatype.ffi.mmodel"]
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


(defn emit-find-symbol
  []
  [[:ldc "tech.v3.datatype.ffi.mmodel"]
   [:ldc "find-symbol"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:aload 0]
   [:getfield :this "libraryImpl" LibraryLookup]
   [:aload 1]
   [:invokeinterface IFn "invoke" [Object Object Object]]
   [:astore 2]
   [:ldc "tech.v3.datatype.ffi"]
   [:ldc "->pointer"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:aload 2]
   [:invokeinterface IFn "invoke" [Object Object]]
   [:checkcast Pointer]
   [:areturn]])


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
    :interfaces [Library]
    :fields (->> (concat
                  [{:name "asPointer"
                    :type IFn
                    :flags #{:public :final}}
                   {:name "asPointerQ"
                    :type IFn
                    :flags #{:public :final}}
                   {:name "fnMap"
                    :type Object
                    :flags #{:public :final}}
                   {:name "libraryImpl"
                    :type LibraryLookup
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
           {:name :findSymbol
            :flags #{:public}
            :desc [String Pointer]
            :emit (emit-find-symbol)}
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


(defn platform-ptr->ptr
  [arg-idx]
  [[:aload arg-idx]
   [:invokeinterface MemoryAddress "toRawLongValue" [:long]]
   [:invokestatic Pointer "constructNonZero" [:long Pointer]]])


(defn define-foreign-interface
  [rettype argtypes options]
  (let [classname (or (:classname options)
                      (symbol (str "tech.v3.datatype.ffi.mmodel.ffi_"
                                   (name (gensym)))))]
    (let [retval (ffi-base/define-foreign-interface classname rettype argtypes
                   {:src-ns-str "tech.v3.datatype.ffi.mmodel"
                    :platform-ptr->ptr platform-ptr->ptr
                    :ptrtype MemoryAddress})
          iface-cls (:foreign-iface-class retval)
          lookup (MethodHandles/lookup)
          sig {:rettype rettype
               :argtypes argtypes}]
      (assoc retval
             :method-handle (.findVirtual lookup iface-cls "invoke"
                                          (sig->method-type
                                           {:rettype rettype
                                            :argtypes argtypes}))
             :fndesc (sig->fdesc sig)))))


(defn foreign-interface-instance->c
  [iface-def inst]
  (let [linker (CLinker/getInstance)
        new-hdn (.bindTo ^MethodHandle (:method-handle iface-def) inst)
        mem-seg (.upcallStub linker new-hdn ^FunctionDescriptor (:fndesc iface-def))]
    (ffi/->pointer mem-seg)))


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library
              :define-foreign-interface define-foreign-interface
              :foreign-interface-instance->c foreign-interface-instance->c})
