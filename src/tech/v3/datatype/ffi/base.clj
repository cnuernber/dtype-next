(ns tech.v3.datatype.ffi.base
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [insn.core :as insn])
  (:import [clojure.lang IFn RT Keyword]
           [tech.v3.datatype ClojureHelper NumericConversions]
           [tech.v3.datatype.ffi Pointer]
           [java.util HashMap]))


(defn unify-ptr-types
  [argtype]
  (if (= argtype :pointer?)
    :pointer
    argtype))


(defn argtype->insn
  [platform-ptr-type ptr-disposition argtype]
  (case (-> (ffi-size-t/lower-type argtype)
            (unify-ptr-types))
    :int8 :byte
    :int16 :short
    :int32 :int
    :int64 :long
    :float32 :float
    :float64 :double
    :pointer
    (case ptr-disposition
      :ptr-as-int (argtype->insn platform-ptr-type
                                 :ptr-as-int (ffi-size-t/size-t-type))
      :ptr-as-obj Object
      :ptr-as-ptr tech.v3.datatype.ffi.Pointer
      :ptr-as-platform platform-ptr-type)
    :void :void
    (do
      (errors/when-not-errorf
       (instance? Class argtype)
       "Argument type %s is unrecognized"
       argtype)
      argtype)))


(defn make-ptr-cast
  [mem-fn-name ptr-type]
  [[:aload 0]
   [:getfield :this mem-fn-name IFn]
   ;;Swap the IFn 'this' ptr and the argument on the stack
   [:swap]
   [:invokeinterface IFn "invoke" [Object Object]]
   [:checkcast ptr-type]])


(defn ptr-return
  [ptr->long]
  (vec
   (concat
    ptr->long
    [[:invokestatic Pointer "constructNonZero" [:long Pointer]]
     [:areturn]])))


(defn find-ptr-ptrq
  [src-namespace]
  [[:aload 0]
   [:ldc src-namespace]
   [:ldc "ptr-value"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:putfield :this "asPointer" IFn]
   [:aload 0]
   [:ldc src-namespace]
   [:ldc "ptr-value-p"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:putfield :this "asPointerQ" IFn]])

(defn args->indexes-args
  [argtypes]
  (->> argtypes
       (reduce (fn [[retval offset] argtype]
                 [(conj retval [offset argtype])
                  (+ (long offset)
                     (long (case (ffi-size-t/lower-type argtype)
                             :int64 2
                             :float64 2
                             1)))])
               [[] 1])
       (first)))

(defn emit-invoker-invoke
  [classname fn-name rettype argtypes]
  (concat
   [[:aload 0]
    [:getfield :this "library" classname]]
   (map-indexed
    (fn [idx argtype]
      (let [arg-idx (inc idx)]
        (case (ffi-size-t/lower-type argtype)
          :int8 [[:aload arg-idx]
                 [:invokestatic NumericConversions "numberCast" [Object Number]]
                 [:invokestatic RT "uncheckedByteCast" [Object :byte]]]
          :int16 [[:aload arg-idx]
                  [:invokestatic NumericConversions "numberCast" [Object Number]]
                  [:invokestatic RT "uncheckedShortCast" [Object :short]]]
          :int32 [[:aload arg-idx]
                  [:invokestatic NumericConversions "numberCast" [Object Number]]
                  [:invokestatic RT "uncheckedIntCast" [Object :int]]]
          :int64 [[:aload arg-idx]
                  [:invokestatic NumericConversions "numberCast" [Object Number]]
                  [:invokestatic RT "uncheckedLongCast" [Object :long]]]
          :float32 [[:aload arg-idx]
                    [:invokestatic NumericConversions "numberCast" [Object Number]]
                    [:invokestatic RT "uncheckedFloatCast" [Object :float]]]
          :float64 [[:aload arg-idx]
                    [:invokestatic NumericConversions "numberCast" [Object Number]]
                    [:invokestatic RT "uncheckedDoubleCast" [Object :double]]]
          [[:aload arg-idx]])))
    argtypes)
   [[:invokevirtual classname fn-name
     (concat (map (partial argtype->insn nil :ptr-as-obj) argtypes)
             [(argtype->insn nil :ptr-as-ptr rettype)])]]
   (case (ffi-size-t/lower-type rettype)
     :int8 [[:invokestatic RT "box" [:byte Number]]]
     :int16 [[:invokestatic RT "box" [:short Number]]]
     :int32 [[:invokestatic RT "box" [:int Number]]]
     :int64 [[:invokestatic RT "box" [:long Number]]]
     :float32 [[:invokestatic RT "box" [:float Number]]]
     :float64 [[:invokestatic RT "box" [:double Number]]]
     :void [[:aconst-null]]
     nil)
   [[:areturn]]))


(defn invoker-classname
  [classname fn-name]
  (symbol (str (name classname) "$invoker_" (name fn-name))))


(defn emit-invokers
  [classname fn-defs]
  (map
   (fn [[fn-name {:keys [rettype argtypes]}]]
     {:name (invoker-classname classname fn-name)
      :flags #{:public}
      :interfaces #{IFn}
      :fields [{:name "library"
                :flags #{:public :final}
                :type classname}]
      :methods [{:name :init
                 :flags #{:public}
                 :desc [classname :void]
                 :emit [[:aload 0]
                        [:invokespecial :super :init [:void]]
                        [:aload 0]
                        [:aload 1]
                        [:putfield :this "library" classname]
                        [:return]]}
                {:name "invoke"
                 :desc (repeat (inc (count argtypes)) Object)
                 :flags #{:public}
                 :emit (emit-invoker-invoke classname fn-name rettype argtypes)}]})
   fn-defs))


(defn emit-library-fn-map
  [classname fn-defs]
  {:name "buildFnMap"
   :desc [Object]
   :emit
   (concat
    [[:new HashMap]
     [:dup]
     [:invokespecial HashMap :init [:void]]
     [:astore 1]]
    (mapcat
     (fn [[fn-name _fn-data]]
       [[:aload 1]
        [:ldc (name fn-name)]
        [:invokestatic Keyword "intern" [String Keyword]]
        [:new (invoker-classname classname fn-name)]
        [:dup]
        [:aload 0]
        [:invokespecial (invoker-classname classname fn-name) :init [classname :void]]
        [:invokevirtual HashMap 'put [Object Object Object]]
        [:pop]])
     fn-defs)
    [[:aload 1]
     [:areturn]])})


(defn emit-library-symbol-table
  [_classname symbols find-symbol-fn]
  {:name "buildSymbolTable"
   :desc [Object]
   :emit
   (concat
    [[:new HashMap]
     [:dup]
     [:invokespecial HashMap :init [:void]]
     [:astore 1]]
    (mapcat
     (fn [symbol-name]
       (concat
        [[:aload 1]
         [:ldc (name symbol-name)]
         [:invokestatic Keyword "intern" [String Keyword]]]
        (find-symbol-fn symbol-name)
        [[:invokevirtual HashMap 'put [Object Object Object]]
         [:pop]]))
     symbols)
    [[:aload 1]
     [:areturn]])})


(defn lower-fn-defs
  "Eliminate size-t as a type and if the arg type is a tuple take remove
  the argname."
  [fn-defs]
  (->> fn-defs
       (map (fn [[fn-name fn-data]]
              [fn-name (update fn-data :argtypes
                               #(mapv (fn [fn-item]
                                        (-> (if (sequential? fn-item)
                                              (second fn-item)
                                              fn-item)
                                            (ffi-size-t/lower-type)))
                                      %))]))
       (into {})))


(defn define-library
  [fn-defs symbols classname library-classes-fn instantiate? options]
  (let [fn-defs (lower-fn-defs fn-defs)
        lib-class-defs (library-classes-fn classname fn-defs symbols options)
        n-lib-class-defs (count lib-class-defs)
        class-definitions (->> (concat lib-class-defs
                                       (emit-invokers classname fn-defs))
                               ;;side effects
                               (mapv (fn [cls]
                                       (-> (insn/visit cls)
                                           (insn/write))
                                      ;;defined immediately for repl access
                                       (if instantiate?
                                         (insn/define cls)
                                         (:name cls)))))]
    ;; First we define the inner class which contains the typesafe static methods
    {:library-symbol classname
     :library-class (nth class-definitions (dec n-lib-class-defs))}))


(defn emit-fi-constructor
  [_src-ns-str]
  [[:aload 0]
   [:invokespecial :super :init [:void]]
   [:aload 0]
   [:aload 1]
   [:putfield :this "ifn" IFn]
   [:return]])


(defn ptr->platform-ptr
  [src-ns-str ptrtype ptr-field]
  [[:invokestatic (symbol (str src-ns-str "$" ptr-field)) 'invokeStatic
    [Object Object]]
   [:checkcast ptrtype]
   [:areturn]])


(defn load-ffi-args
  [ptr-cast ptr?-cast argtypes]
  (->> (args->indexes-args argtypes)
       (mapcat
        (fn [[arg-idx argtype]]
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
            :float64 [[:dload arg-idx]])))))


(defn exact-type-retval
  [rettype ptr->ptr]
  (case (ffi-size-t/lower-type rettype)
     :int8 [[:ireturn]]
     :int16 [[:ireturn]]
     :int32 [[:ireturn]]
     :int64 [[:lreturn]]
     :float32 [[:freturn]]
     :float64 [[:dreturn]]
     :void [[:return]]
     :pointer (ptr->ptr "ptr_value")
     :pointer? (ptr->ptr "ptr_value_q")))


(defn object->exact-type-retval
  [rettype ptr->ptr]
  (case (ffi-size-t/lower-type rettype)
    :void [[:return]]
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
    :pointer (ptr->ptr "ptr_value")
    :pointer? (ptr->ptr "ptr_value_q")))


(defn emit-fi-invoke
  [platform-ptr->ptr ptr->platform-ptr rettype argtypes]
  (concat
   [[:aload 0]
    [:getfield :this "ifn" IFn]]
   (->> (args->indexes-args argtypes)
        (mapcat (fn [[arg-idx argtype]]
                  (case (ffi-size-t/lower-type argtype)
                    :int8 [[:iload arg-idx]
                           [:invokestatic RT "box" [:byte Number]]]
                    :int16 [[:iload arg-idx]
                            [:invokestatic RT "box" [:short Number]]]
                    :int32 [[:iload arg-idx]
                            [:invokestatic RT "box" [:int Number]]]
                    :int64 [[:lload arg-idx]
                            [:invokestatic RT "box" [:long Number]]]
                    :float32 [[:fload arg-idx]
                              [:invokestatic RT "box" [:float Number]]]
                    :float64 [[:dload arg-idx]
                              [:invokestatic RT "box" [:double Number]]]
                    :pointer (platform-ptr->ptr arg-idx)
                    :pointer? (platform-ptr->ptr arg-idx arg-idx)))))
   [[:invokeinterface IFn "invoke" (repeat (inc (count argtypes)) Object)]]
   (object->exact-type-retval rettype ptr->platform-ptr)))



(defn foreign-interface-definition
  [iface-symbol rettype argtypes {:keys [ptr-resolve-ns ;;ns string
                                         platform-ptr->ptr
                                         ptr->platform-ptr ;;convert to a platform ptr
                                         ptrtype ;;platform ptr type
                                         interfaces ;;marker interfaces
                                         ]}]
  (merge
   {:name iface-symbol
    :flags #{:public}
    :fields [{:name "ifn"
              :type IFn
              :flags #{:public :final}}]
    :methods [{:name :init
               :flags #{:public}
               :desc [IFn :void]
               :emit (emit-fi-constructor ptr-resolve-ns)}
              {:name :invoke
               :flags #{:public}
               :desc (vec
                      (concat (map (partial argtype->insn ptrtype
                                            :ptr-as-platform) argtypes)
                              [(argtype->insn ptrtype :ptr-as-platform rettype)]))
               :emit (emit-fi-invoke platform-ptr->ptr
                                     ptr->platform-ptr
                                     rettype argtypes)}]}
   (when interfaces
     {:interfaces interfaces})))


(defn define-foreign-interface
  [classname rettype argtypes options]
  (let [cls-def (foreign-interface-definition classname rettype argtypes options)]
    (-> cls-def
        (insn/visit)
        (insn/write))
    {:rettype rettype
     :argtypes argtypes
     :foreign-iface-symbol classname
     :foreign-iface-class (insn/define cls-def)}))
