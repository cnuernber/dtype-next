(ns tech.v3.datatype.ffi.jna
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi.base :as ffi-base]
            [insn.core :as insn])
  (:import [com.sun.jna NativeLibrary Pointer Callback CallbackReference]
           [tech.v3.datatype NumericConversions ClojureHelper]
           [clojure.lang IFn RT IPersistentMap IObj MapEntry Keyword IDeref
            ISeq]
           [java.lang.reflect Constructor]
           [java.nio.file Paths]))


(set! *warn-on-reflection* true)


(extend-type Pointer
  ffi/PToPointer
  (convertible-to-pointer? [item] true)
  (->pointer [item] (tech.v3.datatype.ffi.Pointer. (Pointer/nativeValue item))))


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


(def ptr-cast (ffi-base/make-ptr-cast "asPointer" Pointer))
(def ptr?-cast (ffi-base/make-ptr-cast "asPointerQ" Pointer))
(def ptr-return (ffi-base/ptr-return
                 [[:invokestatic Pointer "nativeValue" [Pointer :long]]]))


(defn ptr-value
  ^Pointer [item]
  (Pointer. (ffi/ptr-value item)))


(defn ptr-value?
  ^Pointer [item]
  (Pointer. (ffi/ptr-value? item)))


(def emit-ptr-ptrq (ffi-base/find-ptr-ptrq "tech.v3.datatype.ffi.jna"))


(defn emit-library-constructor
  [inner-cls]
  (concat
   [[:aload 0]
    [:invokespecial :super :init [:void]]
    [:ldc inner-cls]
    [:ldc "tech.v3.datatype.ffi.jna"]
    [:ldc "load-library"]
    [:invokestatic ClojureHelper "findFn" [String String IFn]]
    [:aload 1]
    [:invokeinterface IFn "invoke" [Object Object]]
    [:checkcast NativeLibrary]
    [:invokestatic com.sun.jna.Native "register" [Class NativeLibrary :void]]]
   emit-ptr-ptrq
   [[:aload 0]
    [:dup]
    [:invokevirtual :this "buildFnMap" [Object]]
    [:putfield :this "fnMap" Object]
    [:return]]))


(defn emit-wrapped-fn
  [inner-cls [fn-name fn-def]]
  {:name fn-name
   :flags #{:public}
   :desc (->> (concat (map (partial argtype->insn :ptr-as-obj) (:argtypes fn-def))
                      [(argtype->insn :ptr-as-ptr (:rettype fn-def))])
              (vec))
   :emit
   (vec (concat
         (ffi-base/load-ffi-args ptr-cast ptr?-cast (:argtypes fn-def))
         [[:invokestatic inner-cls (name fn-name)
           (argtypes->insn-desc (:argtypes fn-def) (:rettype fn-def)
                                :ptr-as-platform)]]
         (ffi-base/ffi-call-return ptr-return (:rettype fn-def))))})


(defn define-library
  [fn-defs & [{:keys [classname]
               :or {classname (str "tech.v3.datatype.ffi.jna." (name (gensym)))}}]]
  ;; First we define the inner class which contains the typesafe static methods
  (let [inner-name (symbol (str classname "$inner"))
        fn-defs (ffi-base/lower-fn-defs fn-defs)]
    (-> {:name inner-name
         :flags #{:public :static}
         :methods (mapv (fn [[k v]]
                          {:flags #{:public :static :native}
                           :name k
                           :desc (argtypes->insn-desc
                                  (:argtypes v)
                                  (:rettype v)
                                  :ptr-as-platform)})
                        fn-defs)}
        (insn/visit)
        (insn/write))
    (let [invokers (ffi-base/emit-invokers classname fn-defs)
          cls-data
          {:name classname
           :flags #{:public}
           :interfaces [IDeref]
           :fields [{:name "asPointer"
                     :type IFn
                     :flags #{:public :final}}
                    {:name "asPointerQ"
                     :type IFn
                     :flags #{:public :final}}
                    {:name "fnMap"
                     :type Object
                     :flags #{:public :final}}]
           :methods (vec (concat
                          [{:name :init
                            :desc [String :void]
                            :emit (emit-library-constructor inner-name)}
                           {:name "buildFnMap"
                            :desc [Object]
                            :emit (ffi-base/emit-library-fn-map classname fn-defs)}
                           {:name :deref
                            :desc [Object]
                            :emit [[:aload 0]
                                   [:getfield :this "fnMap" Object]
                                   [:areturn]]}]
                          (mapv (partial emit-wrapped-fn inner-name) fn-defs)))}]
      (doseq [cls (concat [cls-data] invokers)]
        (-> cls
            (insn/visit)
            (insn/write))
        (insn/define cls))
      {:library-class (insn/define cls-data)
       :library-symbol classname})))


(defn emit-fi-constructor
  []
  (concat
   [[:aload 0]
    [:invokespecial :super :init [:void]]
    [:aload 0]
    [:aload 1]
    [:putfield :this "ifn" IFn]]
   emit-ptr-ptrq
   [[:return]]))


(defn load-ptr
  [arg-idx]
  [[:new tech.v3.datatype.ffi.Pointer]
   [:dup]
   [:aload arg-idx]
   [:invokestatic Pointer "nativeValue" [Pointer :long]]
   [:invokespecial tech.v3.datatype.ffi.Pointer :init [:long :void]]])


(defn ifn-return-ptr
  [ptr-field]
  [[:astore 1]
   [:new Pointer]
   [:dup]
   [:aload 0]
   [:getfield :this ptr-field IFn]
   [:aload 1]
   [:invokeinterface IFn "invoke" [Object Object]]
   [:checkcast Long]
   [:invokevirtual Long "asLong" [:long]]
   [:invokespecial Pointer :init [:long :void]]
   [:areturn]])


(defn foreign-interface-definition
  [iface-symbol rettype argtypes options]
  {:name iface-symbol
   :flags #{:public}
   :interfaces [Callback]
   :fields [{:name "asPointer"
             :type IFn
             :flags #{:public :final}}
            {:name "asPointerQ"
             :type IFn
             :flags #{:public :final}}
            {:name "ifn"
             :type IFn
             :flags #{:public :final}}]
   :methods [{:name :init
              :flags #{:public}
              :desc [IFn :void]
              :emit (emit-fi-constructor)}
             {:name :invoke
              :flags #{:public}
              :desc (vec
                     (concat (map (partial argtype->insn
                                           :ptr-as-platform) argtypes)
                             [(argtype->insn :ptr-as-platform rettype)]))
              :emit (ffi-base/emit-fi-invoke load-ptr ifn-return-ptr
                                             rettype argtypes)}]})


(defn define-foreign-interface
  [rettype argtypes options]
  (let [classname (or (:classname options)
                      (symbol (str "tech.v3.datatype.ffi.jna.ffi_" (name (gensym)))))
        cls-def (foreign-interface-definition classname rettype argtypes options)]
    (-> cls-def
        (insn/visit)
        (insn/write))
    {:rettype rettype
     :argtypes argtypes
     :foreign-iface-symbol classname
     :foreign-iface-class (insn/define cls-def)}))


(defn foreign-interface-instance->c
  [inst]
  (-> (CallbackReference/getFunctionPointer inst)
      (Pointer/nativeValue)
      (tech.v3.datatype.ffi/Pointer.)))


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library
              :define-foreign-interface define-foreign-interface
              :foreign-interface-instance->c foreign-interface-instance->c})
