(ns tech.v3.datatype.ffi.graalvm
  (:require [tech.v3.datatype.ffi.base :as ffi-base]
            [tech.v3.datatype.ffi.graalvm-runtime])
  (:import [tech.v3.datatype.ffi Library]
           [org.graalvm.word WordBase WordFactory PointerBase]
           [org.graalvm.nativeimage.c.type VoidPointer]
           [org.graalvm.nativeimage.c CContext CContext$Directives]
           [org.graalvm.nativeimage.c.function CFunction]
           [org.graalvm.nativeimage.c.constant CConstant]
           [clojure.lang IFn IDeref RT]
           [java.util ArrayList List]))


(set! *warn-on-reflection* true)


(defn- argtype->insn
  [ptr-disposition argtype]
  (ffi-base/argtype->insn VoidPointer ptr-disposition argtype))


(defn make-ptr-cast
  [ptr-fn]
  [[:invokestatic (symbol (str "tech.v3.datatype.ffi.graalvm_runtime$" ptr-fn))
    'invokeStatic [java.lang.Object java.lang.Object]]
   [:invokestatic RT "uncheckedLongCast" [Object :long]]
   [:invokestatic WordFactory "pointer" [:long PointerBase]]])
(def ptr-cast (make-ptr-cast "ptr_value"))
(def ptr-cast? (make-ptr-cast "ptr_value_q"))
(def ptr-return (ffi-base/ptr-return
                 [[:invokeinterface WordBase "rawValue" [:long]]]))


(def emit-ptr-ptrq (ffi-base/find-ptr-ptrq "tech.v3.datatype.ffi.graalvm-runtime"))


(defn emit-library-constructor
  [inner-cls]
  (concat
   [[:aload 0]
    [:invokespecial :super :init [:void]]]
   [[:aload 0]
    [:dup]
    [:invokevirtual :this "buildFnMap" [Object]]
    [:putfield :this "fnMap" Object]
    [:aload 0]
    [:dup]
    [:invokevirtual :this "buildSymbolTable" [Object]]
    [:putfield :this "symbolTable" Object]
    [:return]]))


(defn emit-constructor-find-symbol
  [inner-name symbol-name]
  [[:invokestatic inner-name (name symbol-name) [VoidPointer]]
   ;;Cannot put word objects into non-word containers
   [:invokeinterface VoidPointer 'rawValue [:long]]
   [:invokestatic RT 'box [:long Number]]])


(defn emit-find-symbol
  []
  [[:aload 1]
   [:aload 0]
   [:getfield :this "symbolTable" Object]
   [:invokestatic 'tech.v3.datatype.ffi.graalvm_runtime$find_library_symbol
    'invokeStatic [Object Object Object]]
   [:checkcast tech.v3.datatype.ffi.Pointer]
   [:areturn]])


(defn emit-wrapped-fn
  [inner-cls [fn-name {:keys [rettype argtypes]}]]
  {:name fn-name
   :flags #{:public}
   :desc (->> (concat (map (partial argtype->insn :ptr-as-obj) argtypes)
                      [(argtype->insn :ptr-as-ptr rettype)])
              (vec))
   :emit
   (vec (concat
         (ffi-base/load-ffi-args ptr-cast ptr-cast? argtypes)
         [[:invokestatic inner-cls (name fn-name)
           (map (partial argtype->insn :ptr-as-platform) (concat argtypes [rettype]))]]
         (ffi-base/exact-type-retval rettype (constantly ptr-return))))})


(defn emit-constant-list
  [method-name list-data]
  {:name method-name
   :flags #{:public}
   :desc [List]
   :emit
   (concat
    [[:new java.util.ArrayList]
     [:dup]
     [:invokespecial java.util.ArrayList :init [:void]]
     [:astore 1]]
    (mapcat (fn [list-item]
              [[:aload 1]
               [:ldc (str list-item)]
               [:invokevirtual java.util.ArrayList "add" [Object :boolean]]
               [:pop]])
            list-data)
    [[:aload 1]
     [:areturn]])})


(defn define-graal-native-library
  [classname fn-defs symbols options]
  ;; First we define the inner class which contains the typesafe static methods
  (let [inner-name (symbol (str classname "$inner"))
        directives-name (symbol (str classname "$directives"))]

    [{:name directives-name
      :interfaces [CContext$Directives]
      :flags #{:public :static}
      :methods (concat
                (when (:header-files options)
                  [(emit-constant-list "getHeaderFiles" (:header-files options))])
                (when (:libraries options)
                  [(emit-constant-list "getLibraries" (:libraries options))]))}
     {:name inner-name
      :annotations {CContext directives-name}
      :flags #{:public :static}
      :methods (concat
                (mapv (fn [sym-name]
                        {:name sym-name
                         :flags #{:public :static :native}
                         :annotations {CConstant (name sym-name)}
                         :desc [VoidPointer]})
                    symbols)
                (mapv (fn [[k v]]
                        {:name k
                         :flags #{:public :static :native}
                         :annotations {CFunction (name k)}
                         :desc (map (partial argtype->insn :ptr-as-platform )
                                    (concat (:argtypes v) [(:rettype v)]))})
                      fn-defs))}
     {:name classname
      :flags #{:public}
      :interfaces [IDeref Library]
      :fields [{:name "fnMap"
                :type Object
                :flags #{:public :final}}
               {:name "symbolTable"
                :type Object
                :flags #{:public :final}}]
      :methods (vec (concat
                     [{:name :init
                       :desc [:void]
                       :emit (emit-library-constructor inner-name)}
                      {:name "findSymbol"
                       :desc [String tech.v3.datatype.ffi.Pointer]
                       :emit (emit-find-symbol)}
                      (ffi-base/emit-library-fn-map classname fn-defs)
                      (ffi-base/emit-library-symbol-table
                       classname symbols (partial emit-constructor-find-symbol
                                                  inner-name))
                      {:name :deref
                       :desc [Object]
                       :emit [[:aload 0]
                              [:getfield :this "fnMap" Object]
                              [:areturn]]}]
                     (mapv (partial emit-wrapped-fn inner-name) fn-defs)))}]))


(defn define-library
  [fn-defs symbols
   {:keys [classname
           instantiate?] :as options}]
  (let [classname (or classname (str "tech.v3.datatype.ffi.graalvm." (name (gensym))))]
    (ffi-base/define-library fn-defs symbols classname define-graal-native-library
      (boolean instantiate?) options)))
