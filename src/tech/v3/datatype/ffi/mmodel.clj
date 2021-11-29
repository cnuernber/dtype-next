(ns tech.v3.datatype.ffi.mmodel
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [tech.v3.datatype.ffi.ptr-value :as ptr-value]
            [tech.v3.datatype.ffi.base :as ffi-base])
  (:import [jdk.incubator.foreign CLinker FunctionDescriptor MemoryLayout
            MemoryAddress Addressable MemoryLayout SymbolLookup
            ResourceScope]
           [java.lang.invoke MethodHandle MethodType MethodHandles]
           [java.nio.file Path Paths]
           [java.util ArrayList]
           [tech.v3.datatype.ffi Pointer Library]
           [clojure.lang Keyword]))


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
  (MemoryAddress/ofLong (ptr-value/ptr-value item)))


(defn ptr-value-q
  ^MemoryAddress [item]
  (MemoryAddress/ofLong (ptr-value/ptr-value? item)))


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
  ^SymbolLookup [libname]
  (cond
    (instance? SymbolLookup libname)
    libname
    (instance? Path libname)
    (do
      (System/load (.toString ^Path libname))
      (SymbolLookup/loaderLookup))
    (string? libname)
    (do
      (let [libname (str libname)]
        (if (or (.contains libname "/")
                (.contains libname "\\"))
          (System/load libname)
          (System/loadLibrary libname)))
      (SymbolLookup/loaderLookup))
    (nil? libname)
    (CLinker/systemLookup)
    :else
    (errors/throwf "Unrecognized libname type %s" (type libname))))


(defn find-symbol
  (^MemoryAddress [libname symbol-name]
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
  (^MemoryAddress [symbol-name]
   (find-symbol nil symbol-name)))


(defn memory-layout-array
  ^"[Ljdk.incubator.foreign.MemoryLayout;" [& args]
  (into-array MemoryLayout args))


(defn argtype->mem-layout-type
  [argtype]
  (case (ffi-size-t/lower-type argtype)
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
  (case (ffi-size-t/lower-type argtype)
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


(defn library-sym-method-handle
  ^MethodHandle [library symbol-name rettype argtypes]
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
    [[:aload 0]
     [:aload 1]
     [:invokestatic 'tech.v3.datatype.ffi.mmodel$load_library
      'invokeStatic [Object Object]]
     [:checkcast SymbolLookup]
     [:putfield :this "libraryImpl" SymbolLookup]]
    ;;Load all the method handles.
    (mapcat
     (fn [[fn-name {:keys [rettype argtypes]}]]
       (let [hdl-name (str (name fn-name) "_hdl")]
         (concat
          [[:aload 0] ;;this-ptr
           [:aload 1] ;;libname
           [:ldc (name fn-name)]
           [:ldc (name rettype)]
           [:invokestatic Keyword "intern" [String Keyword]]
           [:new ArrayList]
           [:dup]
           [:invokespecial ArrayList :init [:void]]
           [:astore 2]]
          (mapcat (fn [argtype]
                    [[:aload 2]
                     [:ldc (name argtype)]
                     [:invokestatic Keyword "intern" [String Keyword]]
                     [:invokevirtual ArrayList 'add [Object :boolean]]
                     [:pop]])
                  argtypes)
          [[:aload 2]
           [:invokestatic 'tech.v3.datatype.ffi.mmodel$library_sym_method_handle
            'invokeStatic
            [Object Object Object Object Object]]
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
  [[:aload 0]
   [:getfield :this "libraryImpl" SymbolLookup]
   [:aload 1]
   [:invokestatic 'tech.v3.datatype.ffi.mmodel$find_symbol
    'invokeStatic [Object Object Object]]
   [:checkcast Addressable]
   [:invokeinterface Addressable 'address [MemoryAddress]]
   [:invokeinterface MemoryAddress 'toRawLongValue [:long]]
   [:invokestatic Pointer 'constructNonZero [:long Pointer]]
   [:areturn]])


(def ptr-cast
  [[:invokestatic 'tech.v3.datatype.ffi.mmodel$ptr_value
    'invokeStatic [Object Object]]
   [:checkcast MemoryAddress]])

(def ptr?-cast
  [[:invokestatic 'tech.v3.datatype.ffi.mmodel$ptr_value_q
    'invokeStatic [Object Object]]
   [:checkcast MemoryAddress]])

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
        (ffi-base/exact-type-retval
         rettype
         (fn [_ptr-type]
           ptr-return)))
       (vec)))


(defn define-mmodel-library
  [classname fn-defs _symbols _options]
  [{:name classname
    :flags #{:public}
    :interfaces [Library]
    :fields (->> (concat
                  [{:name "fnMap"
                    :type Object
                    :flags #{:public :final}}
                   {:name "libraryImpl"
                    :type SymbolLookup
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
  [fn-defs symbols
   {:keys [classname]
    :as options}]
  (let [clsname (or classname (str "tech.v3.datatype.ffi.mmodel." (name (gensym))))]
    (ffi-base/define-library fn-defs symbols clsname define-mmodel-library
      (or (:instantiate? options)
          (not (boolean classname)))
      options)))


(defn platform-ptr->ptr
  [arg-idx]
  [[:aload arg-idx]
   [:invokeinterface MemoryAddress "toRawLongValue" [:long]]
   [:invokestatic Pointer "constructNonZero" [:long Pointer]]])


(defn define-foreign-interface
  [rettype argtypes options]
  (let [classname (or (:classname options)
                      (symbol (str "tech.v3.datatype.ffi.mmodel.ffi_"
                                   (name (gensym)))))
        retval (ffi-base/define-foreign-interface classname rettype argtypes
                 {:src-ns-str "tech.v3.datatype.ffi.mmodel"
                  :platform-ptr->ptr platform-ptr->ptr
                  :ptr->platform-ptr (partial ffi-base/ptr->platform-ptr "tech.v3.datatype.ffi.mmodel" MemoryAddress)
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
           :fndesc (sig->fdesc sig))))


(defn foreign-interface-instance->c
  [iface-def inst]
  (let [linker (CLinker/getInstance)
        new-hdn (.bindTo ^MethodHandle (:method-handle iface-def) inst)
        mem-seg (.upcallStub linker new-hdn ^FunctionDescriptor (:fndesc iface-def)
                             (ResourceScope/globalScope))]
    (ffi/->pointer mem-seg)))


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library
              :define-foreign-interface define-foreign-interface
              :foreign-interface-instance->c foreign-interface-instance->c})
