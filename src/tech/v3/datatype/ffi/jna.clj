(ns tech.v3.datatype.ffi.jna
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.base :as ffi-base]
            [tech.v3.datatype.ffi.ptr-value :as ptr-value]
            [tech.v3.datatype.copy :as dt-copy]
            [clojure.tools.logging :as log])
  (:import [com.sun.jna NativeLibrary Pointer Callback CallbackReference]
           [tech.v3.datatype NumericConversions ClojureHelper]
           [tech.v3.datatype.ffi Library]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [clojure.lang IFn RT IPersistentMap IObj MapEntry Keyword IDeref
            ISeq]
           [java.lang.reflect Constructor]
           [java.nio.file Paths]))


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
    (NativeLibrary/getInstance (str libname))
    (nil? libname)
    (NativeLibrary/getProcess)))


(defn find-symbol
  [libname symbol-name]
  (-> (load-library libname)
      (.getGlobalVariableAddress (str symbol-name))))

(defn argtype->insn
  [ptr-disposition argtype]
  (ffi-base/argtype->insn Pointer ptr-disposition argtype))


(defn argtypes->insn-desc
  ([argtypes rettype ptr-disposition]
   (mapv (partial argtype->insn ptr-disposition) (concat argtypes [rettype])))
  ([argtypes rettype]
   (argtypes->insn-desc argtypes rettype :ptr-as-int)))


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


(defn emit-wrapped-fn
  [inner-cls [fn-name {:keys [argtypes rettype]}]]
  {:name fn-name
   :flags #{:public}
   :desc (->> (concat (map (partial argtype->insn :ptr-as-obj) argtypes)
                      [(argtype->insn :ptr-as-ptr rettype)])
              (vec))
   :emit
   (vec (concat
         (ffi-base/load-ffi-args ptr-cast ptr?-cast argtypes)
         [[:invokestatic inner-cls (name fn-name)
           (argtypes->insn-desc argtypes rettype :ptr-as-platform)]]
         (ffi-base/exact-type-retval rettype
                                     (fn [ptr-fn]
                                       [[:checkcast Pointer]
                                        [:invokestatic Pointer 'nativeValue
                                         [Pointer :long]]
                                        [:invokestatic tech.v3.datatype.ffi.Pointer
                                         'constructNonZero
                                         [:long tech.v3.datatype.ffi.Pointer]]
                                        [:areturn]]))))})


(defn emit-constructor-find-symbol
  [symbol-name]
  [[:aload 0]
   [:getfield :this "libraryImpl" NativeLibrary]
   [:ldc (name symbol-name)]
   [:invokevirtual NativeLibrary "getGlobalVariableAddress" [String Pointer]]])


(defn- inline-print
  [item]
  (clojure.pprint/pprint item)
  item)

(defn define-jna-library
  [classname fn-defs symbols _options]
  ;; First we define the inner class which contains the typesafe static methods
  (let [inner-name (symbol (str classname "$inner"))]
    [{:name inner-name
      :flags #{:public :static}
      :methods (mapv (fn [[k v]]
                       {:flags #{:public :static :native}
                        :name k
                        :desc (argtypes->insn-desc
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
                     (mapv (partial emit-wrapped-fn inner-name) fn-defs)))}]))


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
