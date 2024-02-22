(ns tech.v3.datatype.ffi.mmodel-jdk21
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.base :as ffi-base]
            [tech.v3.datatype.ffi.ptr-value :as ptr-value]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [tech.v3.datatype.ffi.libpath :as libpath])
  (:import [clojure.lang Keyword]
           [java.lang.foreign
             FunctionDescriptor Linker Linker$Option MemoryLayout Arena MemorySegment
             SymbolLookup ValueLayout]
           [java.lang.invoke MethodHandle MethodHandles MethodType]
           [java.nio.file Path Paths]
           [java.util ArrayList]
           [tech.v3.datatype.ffi Pointer Library]))

(set! *warn-on-reflection* true)

(defn ptr-value
  ^MemorySegment [item]
  (MemorySegment/ofAddress (ptr-value/ptr-value item)))

(defn ptr-value-q
  ^MemorySegment [item]
  (MemorySegment/ofAddress (ptr-value/ptr-value? item)))

(extend-protocol ffi/PToPointer
  MemorySegment
    (convertible-to-pointer? [item] true)
    (->pointer [item]
      (Pointer. (.address item) {:src-ptr item})))

(defn ->path
  ^Path [str-base & args]
  (Paths/get (str str-base) (into-array String args)))

(defn load-library
  ^SymbolLookup [libname]
  (cond
    (instance? SymbolLookup libname)
    libname
    (instance? Path libname)
    (do (System/load (.toString ^Path libname))
        (SymbolLookup/loaderLookup))
    (string? libname)
    (libpath/load-library! (fn [libname]
                             (let [libname (str libname)]
                               (if (or (.contains libname "/")
                                       (.contains libname "\\"))
                                 (System/load libname)
                                 (System/loadLibrary libname))
                               (SymbolLookup/loaderLookup)))
                           #(instance? SymbolLookup %)
                           libname)
    (nil? libname)
    (.defaultLookup (Linker/nativeLinker))
    :else
    (errors/throwf "Unrecognized libname type %s" (type libname))))

(defn find-symbol
  (^MemorySegment [libname symbol-name]
   (let [symbol-name (cond
                       (symbol? symbol-name)  (name symbol-name)
                       (keyword? symbol-name) (name symbol-name)
                       :else                  (str symbol-name))]
     (-> (load-library libname)
         (.find symbol-name)
         (.orElseThrow))))
  (^MemorySegment [symbol-name]
   (find-symbol nil symbol-name)))

(defn memory-layout-array
  ^"[Ljava.lang.foreign.MemoryLayout;" [& args]
  (into-array MemoryLayout args))

(defn argtype->mem-layout-type
  [argtype]
  (if (sequential? argtype)
    (do
      (when-not (= 'by-value (first argtype))
        (throw (RuntimeException. (str "Unrecognized argtype type: " (first argtype)))))
      (let [sdef (dt-struct/)]))
    (case (ffi-size-t/lower-type argtype)
      :int8     ValueLayout/JAVA_BYTE
      :int16    ValueLayout/JAVA_SHORT
      :int32    ValueLayout/JAVA_INT
      :int64    ValueLayout/JAVA_LONG
      :float32  ValueLayout/JAVA_FLOAT
      :float64  ValueLayout/JAVA_DOUBLE
      :pointer? ValueLayout/ADDRESS
      :pointer  ValueLayout/ADDRESS)))

(defn sig->fdesc
  ^FunctionDescriptor [{:keys [rettype argtypes]}]
  (if (or (= :void rettype) (nil? rettype))
      (FunctionDescriptor/ofVoid
        (->> argtypes
             (map argtype->mem-layout-type)
             (apply memory-layout-array)))
      (FunctionDescriptor/of
        (argtype->mem-layout-type rettype)
        (->> argtypes
             (map argtype->mem-layout-type)
             (apply memory-layout-array)))))

(defn argtype->cls
  ^Class [argtype]
  (case (ffi-size-t/lower-type argtype)
    :int8     Byte/TYPE
    :int16    Short/TYPE
    :int32    Integer/TYPE
    :int64    Long/TYPE
    :float32  Float/TYPE
    :float64  Double/TYPE
    :pointer  MemorySegment
    :pointer? MemorySegment
    :string   MemorySegment
    :void     Void/TYPE
    (do
      (errors/when-not-errorf
        (instance? Class argtype)
        "argtype (%s) must be instance of class"
        argtype)
      argtype)))

(defn sig->method-type
  ^MethodType [{:keys [rettype argtypes]}]
  (let [^"[Ljava.lang.Class;" cls-ary (->> argtypes
                                           (map argtype->cls)
                                           (into-array Class))]
    (MethodType/methodType (argtype->cls rettype) cls-ary)))

(defn library-sym-method-handle
  ^MethodHandle [library symbol-name rettype argtypes]
  (.downcallHandle (Linker/nativeLinker)
                   (find-symbol library symbol-name)
                   (sig->fdesc {:rettype  rettype
                                :argtypes argtypes})
                   (make-array Linker$Option 0)))

(defn by-value-arg
  [argname]
  (list 'by-value (keyword argname)))

(defn- push-arg
  [arg]
  (if (keyword? arg)
    [[:ldc (name arg)]
     [:invokestatic Keyword "intern" [String Keyword]]]
    (let [[argtype argname] arg]
      (when-not (= argtype 'by-value)
        (throw (RuntimeException. (str "Invalid argument type: " argtype))))
      [[:ldc (name argname)]
       [:invokestatic 'tech.v3.datatype.ffi.mmodel_jdk21$by_value_arg
        'invokeStatic [Object Object]]])))

(defn emit-lib-constructor
  [fn-defs]
  (->>
   (concat
    [[:aload 0]
     [:invokespecial :super :init [:void]]]
    [[:aload 0]
     [:aload 1]
     [:invokestatic 'tech.v3.datatype.ffi.mmodel_jdk21$load_library
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
           [:ldc (name fn-name)]]
          (push-arg rettype)
          [[:new ArrayList]
           [:dup]
           [:invokespecial ArrayList :init [:void]]
           [:astore 2]]
          (mapcat (fn [argtype]
                    (concat 
                     [[:aload 2]]
                     (push-arg argtype)
                     [[:invokevirtual ArrayList 'add [Object :boolean]]
                      [:pop]]))
                  argtypes)
          [[:aload 2]
           [:invokestatic 'tech.v3.datatype.ffi.mmodel_jdk21$library_sym_method_handle
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
    [:invokestatic 'tech.v3.datatype.ffi.mmodel_jdk21$find_symbol
      'invokeStatic [Object Object Object]]
    [:checkcast MemorySegment]
    [:invokeinterface MemorySegment 'address [:long]]
    [:invokestatic Pointer 'constructNonZero [:long Pointer]]
    [:areturn]])


(def ptr-cast
  [[:invokestatic 'tech.v3.datatype.ffi.mmodel_jdk21$ptr_value
      'invokeStatic [Object Object]]
    [:checkcast MemorySegment]])

(def ptr?-cast
  [[:invokestatic 'tech.v3.datatype.ffi.mmodel_jdk21$ptr_value_q
      'invokeStatic [Object Object]]
    [:checkcast MemorySegment]])

(def ptr-return
  (ffi-base/ptr-return
    [[:invokeinterface MemorySegment "address" [:long]]]))

(defn emit-fn-def
  [hdl-name rettype argtypes]
  (->> (concat
         [[:aload 0]
           [:getfield :this hdl-name MethodHandle]]
         (ffi-base/load-ffi-args ptr-cast ptr?-cast argtypes)
         [[:invokevirtual MethodHandle "invokeExact"
             (concat (map (partial ffi-base/argtype->insn
                                   MemorySegment
                                   :ptr-as-platform)
                       argtypes)
                     [(ffi-base/argtype->insn MemorySegment
                                               :ptr-as-platform
                                               rettype)])]]
         (ffi-base/exact-type-retval
           rettype
           (fn [_ptr-type]
             ptr-return)))
       (vec)))

(defn define-mmodel-library
  [classname fn-defs _symbols _options]
  [{:name       classname
     :flags      #{:public}
     :interfaces [Library]
     :fields     (->> (concat
                        [{:name  "fnMap"
                           :type  Object
                           :flags #{:public :final}}
                          {:name  "libraryImpl"
                           :type  SymbolLookup
                           :flags #{:public :final}}]
                        (map (fn [[fn-name _fn-args]]
                               {:name  (str (name fn-name) "_hdl")
                                :type  MethodHandle
                                :flags #{:public :final}})
                          fn-defs))
                      (vec))
     :methods
     (->> (concat
            [{:name  :init
               :flags #{:public}
               :desc  [String :void]
               :emit  (emit-lib-constructor fn-defs)}
              {:name  :findSymbol
               :flags #{:public}
               :desc  [String Pointer]
               :emit  (emit-find-symbol)}
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
                  {:name  fn-name
                   :flags #{:public}
                   :desc  (concat (map (partial ffi-base/argtype->insn
                                                MemorySegment
                                                :ptr-as-obj)
                                    argtypes)
                                  [(ffi-base/argtype->insn MemorySegment
                                                            :ptr-as-ptr
                                                            rettype)])
                   :emit  (emit-fn-def hdl-name rettype argtypes)}))
              fn-defs))
          (vec))}])

(defn define-library
  [fn-defs symbols
    {:keys [classname]
     :as   options}]
  (let [clsname (or classname (str "tech.v3.datatype.ffi.mmodel." (name (gensym))))]
    (ffi-base/define-library fn-defs
                             symbols
                             clsname
                             define-mmodel-library
                             (or (:instantiate? options)
                                 (not (boolean classname)))
                             options)))

(defn platform-ptr->ptr
  [arg-idx]
  [[:aload arg-idx]
    [:invokeinterface MemorySegment "address" [:long]]
    [:invokestatic Pointer "constructNonZero" [:long Pointer]]])

(defn define-foreign-interface
  [rettype argtypes options]
  (let [classname (or (:classname options)
                      (symbol (str "tech.v3.datatype.ffi.mmodel.ffi_"
                                   (name (gensym)))))
        retval    (ffi-base/define-foreign-interface classname
                                                     rettype
                                                     argtypes
                                                     {:src-ns-str "tech.v3.datatype.ffi.mmodel"
                                                      :platform-ptr->ptr platform-ptr->ptr
                                                      :ptr->platform-ptr
                                                      (partial ffi-base/ptr->platform-ptr
                                                               "tech.v3.datatype.ffi.mmodel"
                                                               MemorySegment)
                                                      :ptrtype MemorySegment})
        iface-cls (:foreign-iface-class retval)
        lookup    (MethodHandles/lookup)
        sig       {:rettype  rettype
                   :argtypes argtypes}]
    (assoc retval
      :method-handle (.findVirtual lookup
                                   iface-cls
                                   "invoke"
                                   (sig->method-type sig))
      :fndesc        (sig->fdesc sig))))

(defn foreign-interface-instance->c
  [iface-def inst]
  (let [linker  (Linker/nativeLinker)
        new-hdn (.bindTo ^MethodHandle (:method-handle iface-def) inst)
        mem-seg (.upcallStub
                  linker
                  new-hdn
                  ^FunctionDescriptor (:fndesc iface-def)
                  (Arena/global)
                  (make-array Linker$Option 0))]
    (ffi/->pointer mem-seg)))

(def ffi-fns
  {:load-library                  load-library
   :find-symbol                   find-symbol
   :define-library                define-library
   :define-foreign-interface      define-foreign-interface
   :foreign-interface-instance->c foreign-interface-instance->c})
