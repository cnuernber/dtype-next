(ns tech.v3.datatype.ffi.base
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi])
  (:import [clojure.lang IFn RT IDeref ISeq Keyword]
           [tech.v3.datatype ClojureHelper NumericConversions]))


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
    [[:lstore 1]
     [:new tech.v3.datatype.ffi.Pointer]
     [:dup]
     [:lload 1]
     [:invokespecial tech.v3.datatype.ffi.Pointer :init [:long :void]]
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
  [load-ptr-fn ifn-return-ptr rettype argtypes]
  (concat
   [[:aload 0]
    [:getfield :this "ifn" IFn]]
   (->> (args->indexes-args argtypes)
        (mapcat (fn [[arg-idx argtype]]
                  (case (ffi/lower-type argtype)
                    :int8 [[:iload arg-idx]
                           [:invokestatic RT "box" [:byte Object]]]
                    :int16 [[:iload arg-idx]
                            [:invokestatic RT "box" [:short Object]]]
                    :int32 [[:iload arg-idx]
                            [:invokestatic RT "box" [:int Object]]]
                    :int64 [[:lload arg-idx]
                            [:invokestatic RT "box" [:long Object]]]
                    :float32 [[:fload arg-idx]
                              [:invokestatic RT "box" [:float Object]]]
                    :float64 [[:dload arg-idx]
                              [:invokestatic RT "box" [:double Object]]]
                    :pointer (load-ptr-fn arg-idx)
                    :pointer? (load-ptr-fn arg-idx)))))
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
     :pointer (ifn-return-ptr "asPointer")
     :pointer? (ifn-return-ptr "asPointerQ"))))


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
     :int8 [[:invokestatic RT "box" [:byte Object]]]
     :int16 [[:invokestatic RT "box" [:short Object]]]
     :int32 [[:invokestatic RT "box" [:int Object]]]
     :int64 [[:invokestatic RT "box" [:long Object]]]
     :float32 [[:invokestatic RT "box" [:float Object]]]
     :float64 [[:invokestatic RT "box" [:double Object]]]
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
    [:areturn]]))


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
