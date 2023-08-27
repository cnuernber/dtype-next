(ns tech.v3.datatype.ffi.jna
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.base :as ffi-base]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [tech.v3.datatype.ffi.ptr-value :as ptr-value]
            [tech.v3.datatype.ffi.libpath :as libpath]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.struct :as dt-struct]
            [ham-fisted.api :as hamf]
            [clojure.tools.logging :as log])
  (:import [com.sun.jna NativeLibrary Pointer Callback CallbackReference
            Structure Structure$ByValue]
           [clojure.lang IDeref RT Keyword]
           [java.util Map HashMap]
           [tech.v3.datatype.ffi Library]))


(set! *warn-on-reflection* true)


(extend-type Pointer
  ffi/PToPointer
  (convertible-to-pointer? [item] true)
  (->pointer [item] (tech.v3.datatype.ffi.Pointer. (Pointer/nativeValue item)
                                                   {:src-ptr item})))


(defn library-instance?
  [item]
  (instance? NativeLibrary item))


(defn load-library
  ^NativeLibrary [libname]
  (cond
    (instance? NativeLibrary libname) libname
    (string? libname)
    (libpath/load-library! #(NativeLibrary/getInstance (str %))
                           #(instance? NativeLibrary %)
                           libname)
    (nil? libname)
    (NativeLibrary/getProcess)))


(defn find-symbol
  [libname symbol-name]
  (-> (load-library libname)
      (.getGlobalVariableAddress (str symbol-name))))

(defn argtype->insn
  [classname ptr-disposition argtype]
  (if (sequential? argtype)
    (let [_ (when-not (== 2 (count argtype))
              (throw (Exception. (str "Unrecognized argtype: " argtype))))
          [ptype dtype] argtype]
      (when-not (= 'by-value ptype)
        (throw (Exception. (str "Unrecognized argtype: " argtype))))
      (str classname "$" (munge (name dtype))))
    (ffi-base/argtype->insn Pointer ptr-disposition argtype)))


(defn argtypes->insn-desc
  ([classname argtypes rettype ptr-disposition]
   (mapv (partial argtype->insn classname ptr-disposition) (concat argtypes [rettype])))
  ([classname argtypes rettype]
   (argtypes->insn-desc classname argtypes rettype :ptr-as-int)))


(def ptr-cast
  [[:invokestatic 'tech.v3.datatype.ffi.jna$ptr_value 'invokeStatic [Object Object]]
   [:checkcast Pointer]])
(def ptr?-cast
  [[:invokestatic 'tech.v3.datatype.ffi.jna$ptr_value_q 'invokeStatic [Object Object]]
   [:checkcast Pointer]])
(def ptr-return (ffi-base/ptr-return
                 [[:invokestatic Pointer "nativeValue" [Pointer :long]]]))


(defn ptr-value
  ^Pointer [item]
  (Pointer. (ptr-value/ptr-value item)))


(defn ptr-value-q
  ^Pointer [item]
  (Pointer. (ptr-value/ptr-value? item)))


(def emit-ptr-ptrq (ffi-base/find-ptr-ptrq "tech.v3.datatype.ffi.jna"))


(defn emit-library-constructor
  [inner-cls]
  [[:aload 0]
   [:invokespecial :super :init [:void]]
   [:ldc inner-cls]
   [:aload 1]
   [:invokestatic 'tech.v3.datatype.ffi.jna$load_library
    'invokeStatic [Object Object]]
   [:checkcast NativeLibrary]
   [:astore 2]
   [:aload 2]
   [:invokestatic com.sun.jna.Native "register" [Class NativeLibrary :void]]
   [:aload 0]
   [:aload 2]
   [:putfield :this "libraryImpl" NativeLibrary]
   [:aload 0]
   [:dup]
   [:invokevirtual :this "buildFnMap" [Object]]
   [:putfield :this "fnMap" Object]
   [:aload 0]
   [:dup]
   [:invokevirtual :this "buildSymbolTable" [Object]]
   [:putfield :this "symbolTable" Object]
   [:return]])


(defn find-library-symbol
  ^Pointer [symbol-name symbol-map ^NativeLibrary library]
  (if-let [^Pointer retval (get symbol-map (keyword symbol-name))]
    (tech.v3.datatype.ffi.Pointer/constructNonZero (Pointer/nativeValue retval))
    (do
      (log/warnf "Finding non-predefined symbol \"%s\"" symbol-name)
      (-> (.getGlobalVariableAddress library (str symbol-name))
          (Pointer/nativeValue)
          (tech.v3.datatype.ffi.Pointer/constructNonZero)))))


(defn emit-find-symbol
  []
  [[:aload 1]
   [:aload 0]
   [:getfield :this "symbolTable" Object]
   [:aload 0]
   [:getfield :this "libraryImpl" NativeLibrary]
   [:invokestatic 'tech.v3.datatype.ffi.jna$find_library_symbol
    'invokeStatic [Object Object Object Object]]
   [:checkcast tech.v3.datatype.ffi.Pointer]
   [:areturn]])


(defn load-ffi-args
  [classname argtypes]
  (->> (ffi-base/args->indexes-args argtypes)
       (mapcat
        (fn [[arg-idx argtype]]
          (if (sequential? argtype)
            (let [tname (str classname "$" (munge (name (second argtype))))]
              [[:aload arg-idx]
               [:checkcast java.util.Map]
               [:invokestatic tname "fromMap" [java.util.Map tname]]])
            (case (ffi-size-t/lower-type argtype)
              :int8 [[:iload arg-idx]]
              :int16 [[:iload arg-idx]]
              :int32 [[:iload arg-idx]]
              :int64 [[:lload arg-idx]]
              :pointer (vec (concat [[:aload arg-idx]]
                                    ptr-cast))
              :pointer? (vec (concat [[:aload arg-idx]]
                                     ptr?-cast))
              :float32 [[:fload arg-idx]]
              :float64 [[:dload arg-idx]]))))))


(defn emit-wrapped-fn
  "Wrapped fns are fns that take generic arguments and invoke the appropriate inner
  class methods.  The translation from maps to by-value structs happens here as does the
  translation of data into JNA pointers."
  [classname inner-cls [fn-name {:keys [argtypes rettype]}]]
  {:name fn-name
   :flags #{:public}
   :desc (->> (concat (map (partial ffi-base/argtype->insn Pointer :ptr-as-obj) argtypes)
                      [(ffi-base/argtype->insn Pointer :ptr-as-ptr rettype)])
              (vec))
   :emit
   (vec (concat
         (load-ffi-args classname argtypes)
         [[:invokestatic inner-cls (name fn-name)
           (argtypes->insn-desc classname argtypes rettype :ptr-as-platform)]]
         (if (sequential? rettype)
           [[:invokevirtual (str classname "$" (munge (name (second rettype)))) "toStruct" [Object]]
            [:areturn]]
           (ffi-base/exact-type-retval rettype
                                       (fn [_ptr-fn]
                                         [[:checkcast Pointer]
                                          [:invokestatic Pointer 'nativeValue
                                           [Pointer :long]]
                                          [:invokestatic tech.v3.datatype.ffi.Pointer
                                           'constructNonZero
                                           [:long tech.v3.datatype.ffi.Pointer]]
                                          [:areturn]])))))})


(defn emit-constructor-find-symbol
  [symbol-name]
  [[:aload 0]
   [:getfield :this "libraryImpl" NativeLibrary]
   [:ldc (name symbol-name)]
   [:invokevirtual NativeLibrary "getGlobalVariableAddress" [String Pointer]]])


(defn new-struct
  [dtype]
  (dt-struct/new-struct dtype {:container-type :native-heap
                               :track-type :auto}))


(defn emit-by-value-structs
  [classname fn-defs]
  (let [defined-structs (hamf/java-hashmap)]
    (->> (vals fn-defs)
         (mapcat
          (fn [{:keys [rettype argtypes]}]
            (->> (concat [rettype] argtypes)
                 ;;by-value is indicated by a sequential container with two elements
                 (filter #(and (sequential? %)
                               (not (.get defined-structs (second %)))))
                 (mapv (fn [argtype]
                         (when-not (== 2 (count argtype))
                           (throw (RuntimeException. (str "Unrecognized argument type: " argtype))))
                         (when-not (= (first argtype) 'by-value)
                           (throw (RuntimeException. (str "Only 'by-value argument modifier allowed: " argtype))))
                         (let [dtype (second argtype)
                               sname (str classname "$" (munge (name dtype)))
                               full-sname sname
                               sdef (dt-struct/get-struct-def dtype)
                               layout (->> (sdef :data-layout)
                                           (mapv #(update % :datatype (fn [dt]
                                                                        (argtype->insn classname :ptr-as-int
                                                                                       (casting/datatype->host-type dt))))))
                               retval
                               {:name sname
                                :flags #{:public :static}
                                :super Structure
                                :interfaces [Structure$ByValue]
                                :fields (mapv (fn [{:keys [name datatype offset n-elems]}]
                                                (when-not (== 1 (long n-elems))
                                                  (throw (RuntimeException. "Array properties not supported")))
                                                {:name (munge (clojure.core/name name))
                                                 :type datatype
                                                 :flags #{:public}})
                                              layout)
                                :methods [{:name "getFieldOrder"
                                           :flags [:protected]
                                           :desc [java.util.List]
                                           :emit (concat
                                                  [[:ldc (count layout)]
                                                   [:anewarray String]
                                                   [:astore 1]]
                                                  (->> layout
                                                       (map-indexed (fn [idx {:keys [name]}]
                                                                      (let [name (munge (clojure.core/name name))]
                                                                        [[:aload 1]
                                                                         [:ldc (int idx)]
                                                                         [:ldc name]
                                                                         [:aastore]])))
                                                       (apply concat))
                                                  [[:aload 1]
                                                   [:invokestatic java.util.Arrays 'asList [(type (object-array [])) java.util.List]]
                                                   [:areturn]])}
                                          {:name :fromMap
                                           :flags #{:public :static}
                                           :desc [java.util.Map sname]
                                           :emit (concat
                                                  [[:new full-sname]
                                                   [:dup]
                                                   [:invokespecial full-sname :init [:void]]
                                                   [:astore 1]]
                                                  (mapcat (fn [{:keys [name datatype offset n-elems]}]
                                                            (let [oname (clojure.core/name name)
                                                                  name (munge oname)]
                                                              (concat
                                                               [[:aload 1]
                                                                [:aload 0]
                                                                [:ldc oname]
                                                                [:invokestatic Keyword 'intern [String Keyword]]
                                                                [:invokeinterface java.util.Map 'get [Object Object]]]
                                                               (ffi-base/emit-obj->primitive-cast datatype)
                                                               [[:putfield full-sname name datatype]])))
                                                          layout)
                                                  [[:aload 1]
                                                   [:areturn]])}
                                          {:name :toMap
                                           :flags #{:public}
                                           :desc [java.util.Map java.util.Map]
                                           :emit (concat
                                                  (mapcat (fn [{:keys [name datatype offset n-elems]}]
                                                            (let [oname (clojure.core/name name)
                                                                  name (munge oname)]
                                                              [[:aload 1]
                                                               [:ldc oname]
                                                               [:invokestatic Keyword 'intern [String Keyword]]
                                                               [:aload 0]
                                                               [:getfield :this name datatype]
                                                               [:invokestatic RT "box" [datatype Number]]
                                                               [:invokeinterface java.util.Map 'put [Object Object Object]]]))
                                                          layout)
                                                  [[:aload 1]
                                                   [:areturn]])}
                                          {:name :toStruct
                                           :flags #{:public}
                                           :desc [Object]
                                           :emit [[:aload 0]
                                                  [:ldc (clojure.core/name dtype)]
                                                  [:invokestatic Keyword 'intern [String Keyword]]
                                                  [:invokestatic 'tech.v3.datatype.ffi.jna$new_struct 'invokeStatic
                                                   [Object Object]]
                                                  [:checkcast 'java.util.Map]
                                                  [:invokevirtual :this 'toMap [java.util.Map java.util.Map]]
                                                  [:areturn]]}]}]
                           (.put defined-structs dtype retval)
                           retval))))))
         ;;force errors right here
         (vec))))


(defn define-jna-library
  [classname fn-defs symbols _options]
  ;; First we define the inner class which contains the typesafe static methods
  (let [inner-name (symbol (str classname "$inner"))]
    (hamf/concatv
     (emit-by-value-structs classname fn-defs)
     [{:name inner-name
       :flags #{:public :static}
       :methods (mapv (fn [[k v]]
                        {:flags #{:public :static :native}
                         :name k
                         :desc (argtypes->insn-desc
                                classname
                                (:argtypes v)
                                (:rettype v)
                                :ptr-as-platform)})
                      fn-defs)}
      {:name classname
       :flags #{:public}
       :interfaces [IDeref Library]
       :fields [{:name "libraryImpl"
                 :type NativeLibrary
                 :flags #{:public :final}}
                {:name "fnMap"
                 :type Object
                 :flags #{:public :final}}
                {:name "symbolTable"
                 :type Object
                 :flags #{:public :final}}]
       :methods (vec (concat
                      [{:name :init
                        :desc [String :void]
                        :emit (emit-library-constructor inner-name)}
                       {:name "findSymbol"
                        :desc [String tech.v3.datatype.ffi.Pointer]
                        :emit (emit-find-symbol)}
                       (ffi-base/emit-library-fn-map classname fn-defs)
                       (ffi-base/emit-library-symbol-table classname symbols
                                                           emit-constructor-find-symbol)
                       {:name"deref"
                        :desc [Object]
                        :emit [[:aload 0]
                               [:getfield :this "fnMap" Object]
                               [:areturn]]}]
                      (mapv (partial emit-wrapped-fn classname inner-name) fn-defs)))}])))


(defn define-library
  [fn-defs symbols
   {:keys [classname] :as options}]
  (let [instantiate? (nil? classname)
        classname (or classname (str "tech.v3.datatype.ffi.jna." (name (gensym))))]
    (ffi-base/define-library fn-defs symbols classname define-jna-library
      instantiate? options)))


(defn platform-ptr->ptr
  [arg-idx]
  [[:aload arg-idx]
   [:invokestatic Pointer "nativeValue" [Pointer :long]]
   [:invokestatic tech.v3.datatype.ffi.Pointer "constructNonZero"
    [:long tech.v3.datatype.ffi.Pointer]]])


(defn define-foreign-interface
  [rettype argtypes options]
  (let [classname (or (:classname options)
                      (symbol (str "tech.v3.datatype.ffi.jna.ffi_" (name (gensym))))) ]
    (ffi-base/define-foreign-interface classname rettype argtypes
      {:src-ns-str "tech.v3.datatype.ffi.jna"
       :platform-ptr->ptr platform-ptr->ptr
       :ptr->platform-ptr (partial ffi-base/ptr->platform-ptr "tech.v3.datatype.ffi.jna" Pointer)
       :ptrtype Pointer
       :interfaces [Callback]})))


(defn foreign-interface-instance->c
  [_iface-def inst]
  (let [jna-ptr (CallbackReference/getFunctionPointer inst)]
    ;;make sure we keep the src ptr in scope
    (tech.v3.datatype.ffi.Pointer. (Pointer/nativeValue jna-ptr)
                                   {:src-ptr jna-ptr})))


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library
              :define-foreign-interface define-foreign-interface
              :foreign-interface-instance->c foreign-interface-instance->c})
