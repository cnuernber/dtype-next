(ns tech.v3.datatype.ffi.graalvm
  "Graalvm-specific namespace that implements the dtype-next ffi system and allows users
  to build stand-alone shared libraries that other languages can use to call Clojure code via
  a binary C interface."
  (:require [tech.v3.datatype.ffi.base :as ffi-base]
            [tech.v3.datatype.errors :as errors]
            [insn.core :as insn]
            [tech.v3.datatype.ffi.graalvm-runtime]
            [clojure.string :as s])
  (:import [tech.v3.datatype.ffi Library Pointer]
           [org.graalvm.word WordBase WordFactory PointerBase]
           [org.graalvm.nativeimage.c.type VoidPointer]
           [org.graalvm.nativeimage.c CContext CContext$Directives]
           [org.graalvm.nativeimage.c.function CFunction CEntryPoint]
           [org.graalvm.nativeimage.c.constant CConstant]
           [org.graalvm.nativeimage IsolateThread]
           [clojure.lang IFn IDeref RT]
           [java.util ArrayList List]
           [java.nio.file Paths]))


(set! *warn-on-reflection* true)


(defn- argtype->insn
  [ptr-disposition argtype]
  (ffi-base/argtype->insn VoidPointer ptr-disposition argtype))


(defn- ^:private make-ptr-cast
  [ptr-fn]
  [[:invokestatic (symbol (str "tech.v3.datatype.ffi.graalvm_runtime$" ptr-fn))
    'invokeStatic [java.lang.Object java.lang.Object]]
   [:invokestatic RT "uncheckedLongCast" [Object :long]]
   [:invokestatic WordFactory "pointer" [:long PointerBase]]])
(def ^:private ptr-cast (make-ptr-cast "ptr_value"))
(def ^:private ptr-cast? (make-ptr-cast "ptr_value_q"))
(def ^:private ptr-return (ffi-base/ptr-return
                           [[:invokeinterface WordBase "rawValue" [:long]]]))


(defn- emit-library-constructor
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


(defn- emit-constructor-find-symbol
  [inner-name symbol-name]
  [[:invokestatic inner-name (name symbol-name) [VoidPointer]]
   ;;Cannot put word objects into non-word containers
   [:invokeinterface VoidPointer 'rawValue [:long]]
   [:invokestatic RT 'box [:long Number]]])


(defn- emit-find-symbol
  []
  [[:aload 1]
   [:aload 0]
   [:getfield :this "symbolTable" Object]
   [:invokestatic 'tech.v3.datatype.ffi.graalvm_runtime$find_library_symbol
    'invokeStatic [Object Object Object]]
   [:checkcast tech.v3.datatype.ffi.Pointer]
   [:areturn]])


(defn- emit-wrapped-fn
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


(defn- emit-constant-list
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


(defn- define-graal-native-library
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
  "Define a graal-native set functions bound to a particular native library.  See the
  documentation for [[tech.v3.datatype.ffi/define-library]]."
  [fn-defs symbols
   {:keys [classname
           instantiate?] :as options}]
  (let [classname (or classname (str "tech.v3.datatype.ffi.graalvm." (name (gensym))))]
    (ffi-base/define-library fn-defs symbols classname define-graal-native-library
      (boolean instantiate?) options)))


(defn- map-key->java-safe-ns-name
  [k]
  (let [[ns-name sym-name]
        (if (or (keyword? k)
                (symbol? k))
          [(namespace k) (name k)]
          (let [metadata (meta k)]
            (errors/when-not-errorf
             metadata
             "fn-name %s has no metadat associated.
Ensure you are passing the var \"#'add-fn\" and not the fn \"add-fn\""
             k)
            [(:ns metadata) (:name metadata)]))]
    [(munge ns-name) (munge sym-name)]))


#_(defn- write-java-source
  [fn-defs classname]
  (let [fn-defs (ffi-base/lower-fn-defs fn-defs)
        clsname-parts (s/split (str classname) #"\.")
        package-name (s/join "." (butlast clsname-parts))
        cls-name (last clsname-parts)
        java-fname (-> (Paths/get *compile-path*
                               (into-array
                                String
                                (concat (butlast clsname-parts)
                                        [(str cls-name ".java")])))
                       (str))
        builder (StringBuilder.)
        java-argtype (fn [argtype]
                       (case argtype
                           :int8 "byte"
                           :int16 "short"
                           :int32 "int"
                           :int64 "long"
                           :float32 "float"
                           :float64 "double"
                           :pointer "PointerBase"
                           :pointer? "PointerBase"
                           :void "void"))]
    (.append builder (format "package %s;
import org.graalvm.word.WordBase;
import org.graalvm.word.WordFactory;
import org.graalvm.word.PointerBase;
import org.graalvm.nativeimage.c.function.CEntryPoint;
import org.graalvm.nativeimage.IsolateThread;
import clojure.lang.RT;

public final class %s {
" package-name (last clsname-parts)))

    (doseq [[k {:keys [rettype argtypes]}] fn-defs]
      (let [rettype-name (java-argtype rettype)
            fn-name (name k)
            src-ns (namespace k)]
        (.append builder (format "  @CEntryPoint(name=\"%s\")\n" fn-name))
        (.append builder (format "  public static %s %s(IsolateThread isolate"
                              rettype-name (name k)))
        (doseq [[idx argtype] (map-indexed vector argtypes)]
          (.append builder (format ",%s arg%s"
                                (java-argtype argtype)
                                idx)))
        (.append builder ") {\n")
        (.append builder (format "    Object retval = %s$%s.invokeStatic("
                                 src-ns fn-name))
        (doseq [[idx argtype] (map-indexed vector argtypes)]
          (when (not= 0 (long idx))
            (.append builder ", "))
          (if-not (#{:pointer :pointer?} argtype)
            (.append builder (format "RT.box(arg%s)" idx))
            (.append builder (format "new tech.v3.datatype.ffi.Pointer(arg%s.rawValue())"
                                  idx))))
        (.append builder ");\n")
        (when-not (= rettype :void)
          (.append builder
                (format "    return %s;\n"
                        (case rettype
                          :int8 "RT.uncheckedByteCast(retval)"
                          :int16 "RT.uncheckedShortCast(retval)"
                          :int32 "RT.uncheckedIntCast(retval)"
                          :int64 "RT.uncheckedLongCast(retval)"
                          :float32 "RT.uncheckedFloatCast(retval)"
                          :float64 "RT.uncheckedDoubleCast(retval)"
                          (:pointer :pointer?)

                          (format "WordFactory.pointer(((Pointer)tech.v3.datatype.ffi.graalvm_runtime.%s.invokeStatic(retval)).address)"
                                  (if (= rettype :pointer)
                                    "ptr_value"
                                    "ptr_value_p"))))))
        (.append builder "  }\n")))
    (.append builder "}")
    (spit java-fname (str builder))))


(defn- emit-exposed-fn
  [clojure-sym {:keys [rettype argtypes]}]
  ;;Objects are one slot but args->indexes-args is setup so the 'this' point is the
  ;;first argument and thus starts at arg-position 1
  (let [ptr-fn (fn [ptr-type stack-idx]
                 [[:aload stack-idx]
                  [:invokeinterface PointerBase 'rawValue [:long]]
                  [:invokestatic Pointer 'constructNonZero [:long Pointer]]])]
    (concat
     ;;setup for ifn function call.
     (->> (ffi-base/args->indexes-args argtypes)
          (mapcat (fn [[arg-stack-idx argtype]]
                    (case argtype
                      :int8 [[:iload arg-stack-idx]
                             [:invokestatic RT "box" [:byte Number]]]
                      :int16 [[:iload arg-stack-idx]
                              [:invokestatic RT "box" [:short Number]]]
                      :int32 [[:iload arg-stack-idx]
                              [:invokestatic RT "box" [:int Number]]]
                      :int64 [[:lload arg-stack-idx]
                              [:invokestatic RT "box" [:long Number]]]
                      :float32 [[:fload arg-stack-idx]
                                [:invokestatic RT "box" [:float Number]]]
                      :float64 [[:dload arg-stack-idx]
                                [:invokestatic RT "box" [:double Number]]]
                      :pointer (ptr-fn :pointer arg-stack-idx)
                      :pointer? (ptr-fn :pointer? arg-stack-idx)))))
     [[:invokestatic clojure-sym 'invokeStatic
       (vec (repeat (inc (count argtypes)) Object))]]
     (case rettype
       :void [[:pop]
              [:return]]
       :int8 [[:invokestatic RT "uncheckedByteCast" [Object :byte]]
              [:ireturn]]
       :int16 [[:invokestatic RT "uncheckedShortCast" [Object :short]]
               [:ireturn]]
       :int32 [[:invokestatic RT "uncheckedIntCast" [Object :int]]
               [:ireturn]]
       :int64 [[:invokestatic RT "uncheckedLongCast" [Object :long]]
               [:lreturn]]
       :float32 [[:invokestatic RT "uncheckedFloatCast" [Object :float]]
                 [:freturn]]
       :float64 [[:invokestatic RT "uncheckedDoubleCast" [Object :double]]
                 [:dreturn]]
       (:pointer :pointer?)
       (let [sym (if (= rettype :pointer)
                   'tech.v3.datatype.ffi.graalvm_runtime$ptr_value
                   'tech.v3.datatype.ffi.graalvm_runtime$ptr_value_q)]
         [[:invokestatic sym 'invokeStatic [Object Object]]
          [:invokestatic RT 'uncheckedLongCast [Object :long]]
          [:invokestatic WordFactory 'pointer [:long PointerBase]]
          [:areturn]])))))


(defn- generate-assembly
  [fn-defs classname {:keys [instantiate?]}]
  (let [fn-defs (ffi-base/lower-fn-defs fn-defs)
        clsname-parts (s/split (str classname) #"\.")
        cls-def {:name classname
                 :flags #{:public :final}
                 :source (str (last clsname-parts) ".java")
                 :methods
                 (->>
                  fn-defs
                  (mapv
                   (fn [[k {:keys [argtypes rettype] :as fn-def}]]
                     (let [[sym-ns sym-name] (map-key->java-safe-ns-name k)
                           static-cls (symbol (str sym-ns
                                                   "$"
                                                   sym-name))
                           fn-name (or (:fn-name fn-def) sym-name)]
                       {:name fn-name
                        :flags #{:public :static}
                        :annotations {CEntryPoint {:name (str fn-name)}}
                        :desc (vec (concat
                                    [IsolateThread]
                                    (map (partial argtype->insn
                                                  :ptr-as-platform)
                                         (concat argtypes [rettype]))))
                        :emit (emit-exposed-fn static-cls fn-def)}))))}
        node-type (insn/visit cls-def)]
    (insn/write node-type)
    (if instantiate?
      (insn/new-instance cls-def)
      classname)))


(defn expose-clojure-functions
  "Expose a set of clojure functions as graal library entry points.  In this case,
  the keys of fn-defs are the full namespaced symbols that point to the
  fns you want to define. These will be defined to a class named 'classname' and the
  resuling class file will be output to `*compile-path*`.

  One caveat - strings are passed as tech.v3.datatype.ffi.Pointer classes and you
  will have to use tech.v3.datatype.ffi/c->string in order to process them.

  Any persistent state must be referenced also from your main class in your
  jarfile so it will have to reference systems used in your libfile.  'def', 'defonce'
  variables will show up as uninitialized exceptions at runtime when objects call
  into your library if they are not referenced in some manner from you jar's main
  function.  This can be achieved via several ways, one of which is to have your exposed
  namespace referenced from your main namespace or to have your library export file
  share a common namespace with your main namespace.


  * [avclj symbol export example](https://github.com/cnuernber/avclj/blob/master/native_test/avclj/libavclj.clj).
  * [calling exposed functions from c++](https://github.com/cnuernber/avclj/blob/master/library/testencode.cpp) - note they take an extra parameter, the 'thread isolate'.
  * [calling libsci from rust, c++, python](https://github.com/borkdude/sci/blob/master/doc/libsci.md).


  Once you have exposed a set of clojure functions you can then make those functions
  actual C pointers via the [CEntryPointLiteral class](https://www.graalvm.org/truffle/javadoc/org/graalvm/nativeimage/c/function/CEntryPointLiteral.html).  Please see [issue 28](https://github.com/cnuernber/dtype-next/issues/28) for an excellent walkthrough of how to do this.  Please
  note the exposed function will have an extra parameter, the thread isolate, that your C
  system will have to take care of."
  [fn-defs classname options]
  (generate-assembly fn-defs classname options))
