(ns tech.v3.datatype.ffi.base
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi]
            [insn.core :as insn])
  (:import [clojure.lang IFn RT IDeref ISeq Keyword]
           [tech.v3.datatype ClojureHelper NumericConversions]
           [tech.v3.datatype.ffi Pointer]))


(defn- unchecked-ptr-value
  ^long [item]
  (if item
    (cond
      (instance? tech.v3.datatype.ffi.Pointer item)
      (.address ^tech.v3.datatype.ffi.Pointer item)
      (instance? tech.v3.datatype.native_buffer.NativeBuffer item)
      (.address ^tech.v3.datatype.native_buffer.NativeBuffer item)
      :else
      (do
        (errors/when-not-errorf
         (ffi/convertible-to-pointer? item)
         "Item %s is not convertible to a C pointer" item)
        (.address ^tech.v3.datatype.ffi.Pointer (ffi/->pointer item))))
    0))


(defn ptr-value
  "Item must not be nil.  A long address is returned."
  ^long [item]
  (let [retval (unchecked-ptr-value item)]
    (errors/when-not-error
     (not= 0 retval)
     "Pointer value is zero!")
    retval))


(defn ptr-value?
  "Item may be nil in which case 0 is returned."
  ^long [item]
  (if item
    (unchecked-ptr-value item)
    0))


(defn unify-ptr-types
  [argtype]
  (if (= argtype :pointer?)
    :pointer
    argtype))


(defn argtype->insn
  [platform-ptr-type ptr-disposition argtype]
  (case (-> (ffi/lower-type argtype)
            (unify-ptr-types))
    :int8 :byte
    :int16 :short
    :int32 :int
    :int64 :long
    :float32 :float
    :float64 :double
    :pointer
    (case ptr-disposition
      :ptr-as-int (argtype->insn platform-ptr-type :ptr-as-int (ffi/size-t-type))
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
   [:ldc "ptr-value?"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:putfield :this "asPointerQ" IFn]])

(defn args->indexes-args
  [argtypes]
  (->> argtypes
       (reduce (fn [[retval offset] argtype]
                 [(conj retval [offset argtype])
                  (+ (long offset)
                     (long (case (ffi/lower-type argtype)
                             :int64 2
                             :float64 2
                             1)))])
               ;;this ptr is offset 0
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
        (case (ffi/lower-type argtype)
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
   (case (ffi/lower-type rettype)
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
    [[:new 'java.util.ArrayList]
     [:dup]
     [:invokespecial 'java.util.ArrayList :init [:void]]
     [:astore 1]]
    (mapcat
     (fn [[fn-name _fn-data]]
       [[:aload 1]
        [:ldc (name fn-name)]
        [:invokestatic Keyword "intern" [String Keyword]]
        [:invokevirtual 'java.util.ArrayList "add" [Object :boolean]]
        [:pop]
        [:aload 1]
        [:new (invoker-classname classname fn-name)]
        [:dup]
        [:aload 0]
        [:invokespecial (invoker-classname classname fn-name) :init [classname :void]]
        [:invokevirtual 'java.util.ArrayList "add" [Object :boolean]]
        [:pop]])
     fn-defs)
    [[:ldc "clojure.core"]
     [:ldc "hash-map"]
     [:invokestatic ClojureHelper "findFn" [String String IFn]]
     [:aload 1]
     [:invokestatic RT "seq" [Object ISeq]]
     [:invokeinterface IFn "applyTo" [ISeq Object]]
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
                                            (ffi/lower-type)))
                                      %))]))
       (into {})))


(defn define-library
  [fn-defs classname library-classes-fn]
  (let [fn-defs (lower-fn-defs fn-defs)]
    ;; First we define the inner class which contains the typesafe static methods
    {:library-symbol classname
     :library-class
     (->> (concat (library-classes-fn classname fn-defs)
                  (emit-invokers classname fn-defs))
          ;;side effects
          (mapv (fn [cls]
                  (-> (insn/visit cls)
                      (insn/write))
                  ;;defined immediately for repl access
                  (insn/define cls)))
          (first))}))


(defn emit-fi-constructor
  [src-ns-str]
  (concat
   [[:aload 0]
    [:invokespecial :super :init [:void]]
    [:aload 0]
    [:aload 1]
    [:putfield :this "ifn" IFn]]
   (find-ptr-ptrq src-ns-str)
   [[:return]]))


(defn ptr->platform-ptr
  [ptrtype ptr-field]
  [[:aload 0]
   [:getfield :this ptr-field IFn]
   [:swap]
   [:invokeinterface IFn "invoke" [Object Object]]
   [:checkcast ptrtype]
   [:areturn]])


(defn load-ffi-args
  [ptr-cast ptr?-cast argtypes]
  (->> (args->indexes-args argtypes)
       (mapcat
        (fn [[arg-idx argtype]]
          (case (ffi/lower-type argtype)
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


(defn ffi-call-return
  [ptr-return rettype]
  (case (ffi/lower-type rettype)
    :void [[:return]]
    :int8 [[:ireturn]]
    :int16 [[:ireturn]]
    :int32 [[:ireturn]]
    :int64 [[:lreturn]]
    :pointer ptr-return
    :float32 [[:freturn]]
    :float64 [[:dreturn]]))


(defn emit-fi-invoke
  [platform-ptr->ptr ptr->platform-ptr rettype argtypes]
  (concat
   [[:aload 0]
    [:getfield :this "ifn" IFn]]
   (->> (args->indexes-args argtypes)
        (mapcat (fn [[arg-idx argtype]]
                  (case (ffi/lower-type argtype)
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
   (case (ffi/lower-type rettype)
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
     :pointer (ptr->platform-ptr "asPointer")
     :pointer? (ptr->platform-ptr "asPointerQ"))))



(defn foreign-interface-definition
  [iface-symbol rettype argtypes {:keys [src-ns-str ;;ns string
                                         platform-ptr->ptr
                                         ptrtype ;;platform ptr type
                                         interfaces ;;marker interfaces
                                         ]}]
  (merge
   {:name iface-symbol
    :flags #{:public}
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
               :emit (emit-fi-constructor src-ns-str)}
              {:name :invoke
               :flags #{:public}
               :desc (vec
                      (concat (map (partial argtype->insn ptrtype
                                            :ptr-as-platform) argtypes)
                              [(argtype->insn ptrtype :ptr-as-platform rettype)]))
               :emit (emit-fi-invoke platform-ptr->ptr
                                     (partial ptr->platform-ptr ptrtype)
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
