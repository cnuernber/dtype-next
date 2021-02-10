(ns tech.v3.datatype.ffi.jna
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.errors :as errors]
            [insn.core :as insn])
  (:import [com.sun.jna NativeLibrary Pointer]
           [tech.v3.datatype NumericConversions ClojureHelper]
           [clojure.lang IFn RT IPersistentMap IObj]
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
  (if (instance? NativeLibrary libname)
    libname
    (NativeLibrary/getInstance (str libname))))


(defn find-symbol
  [libname symbol-name]
  (-> (load-library libname)
      (.getGlobalVariableAddress (str symbol-name))))

(defn dtype->insn
  [argtype]
  (case argtype
    :int8 :byte
    :int16 :short
    :int32 :int
    :int64 :long
    :float32 :float
    :float64 :double
    :pointer (dtype->insn (ffi/size-t-type))
    :pointer? (dtype->insn (ffi/size-t-type))
    :size-t (dtype->insn (ffi/size-t-type))
    :void :void
    (do
      (errors/when-not-errorf
       (instance? Class argtype)
       "Argument type %s is unrecognized"
       argtype)
      argtype)))


(defn argtypes->insn-desc
  [argtypes rettype]
  (mapv dtype->insn (concat argtypes [rettype])))


(defn argtypes->obj-insn-desc
  [argtypes rettype]
  (vec (concat (map (fn [argtype]
                      (case argtype
                        :int8 :byte
                        :int16 :short
                        :int32 :int
                        :int64 :long
                        :float32 :float
                        :float64 :double
                        :pointer Object
                        :pointer? Object
                        :size-t (case (ffi/size-t-type)
                                  :int32 :int
                                  :int64 :long)))
                    argtypes)
               [(case rettype
                  :void :void
                  :int8 :byte
                  :int16 :short
                  :int32 :int
                  :int64 :long
                  :float32 :float
                  :float64 :double
                  :pointer tech.v3.datatype.ffi.Pointer
                  :pointer? tech.v3.datatype.ffi.Pointer
                  :size-t (ffi/size-t-type))])))

(def ptr-cast
  (concat [[:aload 0]
           [:getfield :this "asPointer" IFn]
           ;;Swap the IFn 'this' ptr and the argument on the stack
           [:swap]
           [:invokeinterface IFn "invoke" [Object Object]]
           [:checkcast Long]
           [:invokevirtual Long "longValue" [:long]]]
          (when (= :int32 (ffi/size-t-type))
            [[:l2i]])))


(def ptr?-cast
  (concat [[:aload 0]
           [:getfield :this "asPointerQ" IFn]
           ;;Swap the IFn 'this' ptr and the argument on the stack
           [:swap]
           [:invokeinterface IFn "invoke" [Object Object]]
           [:checkcast Long]
           [:invokevirtual Long "longValue" [:long]]]
          (when (= :int32 (ffi/size-t-type))
            [[:l2i]])))


(defn argtype-cast
  [argtype]
  (case argtype
    :int8 [[:invokestatic NumericConversions "numberCast" [Object Number]]
           [:invokestatic RT "uncheckedByteCast" [Object Byte/TYPE]]]
    :int16 [[:invokestatic NumericConversions "numberCast" [Object Number]]
            [:invokestatic RT "uncheckedShortCast" [Object Short/TYPE]]]
    :int32 [[:invokestatic NumericConversions "numberCast" [Object Number]]
            [:invokestatic RT "uncheckedIntCast" [Object Integer/TYPE]]]
    :int64 [[:invokestatic NumericConversions "numberCast" [Object Number]]
            [:invokestatic RT "uncheckedLongCast" [Object Long/TYPE]]]
    :float32 [[:invokestatic NumericConversions "numberCast" [Object Number]]
              [:invokestatic RT "uncheckedFloatCast" [Object Float/TYPE]]]
    :float64 [[:invokestatic NumericConversions "numberCast" [Object Number]]
              [:invokestatic RT "uncheckedDoubleCast" [Object Double/TYPE]]]
    :size-t (argtype-cast (ffi/size-t-type))
    :pointer ptr-cast
    :pointer? ptr-cast))


(def ptr-return
  (concat
   (when (= :int32 (ffi/size-t-type))
     [[:i2l]])
   [[:lstore 1]
    [:new tech.v3.datatype.ffi.Pointer]
    [:dup]
    [:lload 1]
    [:invokespecial tech.v3.datatype.ffi.Pointer :init [:long :void]]
    [:areturn]]))


(defn emit-invoke
  [cls-name symbol-name argtypes rettype]
  (concat
   (->> argtypes
        (map-indexed (fn [idx argtype]
                       (let [arg-idx (inc (long idx))]
                         (concat [[:aload arg-idx]]
                                 (argtype-cast argtype)))))
        (apply concat))
   [[:invokestatic (str "tech.v3.datatype.ffi.jna." cls-name)
     symbol-name (argtypes->insn-desc argtypes rettype)]]
   (case (if (= rettype :size-t)
           (ffi/size-t-type)
           rettype)
     :int8 [[:invokestatic Byte "valueOf" [:byte Byte]]
            [:areturn]]
     :int16 [[:invokestatic Short "valueOf" [:short Short]]
             [:areturn]]
     :int32 [[:invokestatic Integer "valueOf" [:int Integer]]
             [:areturn]]
     :int64 [[:invokestatic Long "valueOf" [:long Long]]
             [:areturn]]
     :float32 [[:invokestatic Float "valueOf" [:float Float]]
               [:areturn]]
     :float64 [[:invokestatic Double "valueOf" [:double Double]]
               [:areturn]]
     :pointer ptr-return
     :pointer? ptr-return
     (if (or (nil? rettype)
             (= :void rettype))
       [[:return]]
       [[:areturn]]))))


(defn generate-class
  [symbol-name rettype argtypes]
  (let [cls-name (str "Invoker_" (name (gensym)))
        full-cls-name (symbol (str "tech.v3.datatype.ffi.jna." cls-name))]
    {:name full-cls-name
     :interfaces [IFn IObj]
     :fields [{:flags #{:public :final}
               :name "asPointer"
               :type IFn}
              {:flags #{:public :final}
               :name "asPointerQ"
               :type IFn}
              {:flags #{:public :final}
               :name "metadata"
               :type IPersistentMap}]
     :methods [{:flags #{:public}
                :name :init
                :desc [IFn IFn IPersistentMap :void]
                :emit [[:aload 0]
                       [:invokespecial :super :init [:void]]
                       [:aload 0]
                       [:aload 1]
                       [:checkcast IFn]
                       [:putfield :this "asPointer" IFn]
                       [:aload 0]
                       [:aload 2]
                       [:checkcast IFn]
                       [:putfield :this "asPointerQ" IFn]
                       [:aload 0]
                       [:aload 3]
                       [:checkcast IFn]
                       [:putfield :this "metadata" IPersistentMap]
                       [:return]]}
               {:flags #{:public :static :native}
                :name symbol-name
                :desc (argtypes->insn-desc argtypes rettype)}
               {:flags #{:public}
                :name :invoke
                :desc (vec (repeat (inc (count argtypes)) Object))
                :emit (emit-invoke cls-name symbol-name argtypes rettype)}
               {:flags #{:public}
                :name :meta
                :desc [IPersistentMap]
                :emit [[:aload 0]
                       [:getfield :this "metadata" IPersistentMap]
                       [:areturn]]}
               {:flags #{:public}
                :name :withMeta
                :desc [IPersistentMap Object]
                :emit [[:new full-cls-name]
                       [:dup]
                       [:aload 0]
                       [:getfield :this "asPointer" IFn]
                       [:aload 0]
                       [:getfield :this "asPointerQ" IFn]
                       [:aload 1]
                       [:invokespecial full-cls-name :init [IFn IFn IPersistentMap :void]]
                       [:areturn]]}
               ]}))


(defn cls->inst
  [invoker-cls]
  (let [^Constructor ctor (first (.getDeclaredConstructors ^Class invoker-cls))]
    (.newInstance ctor (object-array [ffi/ptr-value ffi/ptr-value? {}]))))


(defn generate-binding
  [symbol-name rettype argtypes options]
  (let [cls-def (generate-class symbol-name rettype argtypes)
        cls (insn/define cls-def)
        inst (cls->inst cls)]
    [cls inst]))


(defn find-function
  [libname symbol-name rettype argtypes options]
  (let [library (load-library libname)
        [cls inst] (generate-binding symbol-name rettype argtypes options)]
    (com.sun.jna.Native/register ^Class cls library)
    inst))


(defn emit-library-constructor
  [inner-cls]
  [[:aload 0]
   [:invokespecial :super :init [:void]]
   [:ldc inner-cls]
   [:aload 1]
   [:invokestatic NativeLibrary "getInstance" [String NativeLibrary]]
   [:invokestatic com.sun.jna.Native "register" [Class NativeLibrary :void]]
   [:aload 0]
   [:ldc "tech.v3.datatype.ffi"]
   [:ldc "ptr-value"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:putfield :this "asPointer" IFn]
   [:aload 0]
   [:ldc "tech.v3.datatype.ffi"]
   [:ldc "ptr-value?"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:putfield :this "asPointerQ" IFn]
   [:return]])


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


(defn emit-wrapped-fn
  [inner-cls [fn-name fn-def]]
  {:name fn-name
   :flags #{:public}
   :desc (argtypes->obj-insn-desc (:argtypes fn-def) (:rettype fn-def))
   :emit
   (vec (concat
         (->> (args->indexes-args (:argtypes fn-def))
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
                   :float64 [[:dload arg-idx]]))))
         [[:invokestatic inner-cls (name fn-name)
           (argtypes->insn-desc (:argtypes fn-def) (:rettype fn-def))]
          (case (ffi/lower-type (:rettype fn-def))
            :void [:return]
            :int8 [:ireturn]
            :int16 [:ireturn]
            :int32 [:ireturn]
            :int64 [:lreturn]
            :pointer ptr-return
            :float32 [:freturn]
            :float64 [:dreturn])]))})


(defn define-library
  [library-symbol fn-defs options]
  ;; First we define the inner class which contains the typesafe static methods
  (let [inner-name (symbol (str library-symbol "$inner"))]
    (-> {:name inner-name
         :flags #{:public :static}
         :methods (mapv (fn [[k v]]
                          {:flags #{:public :static :native}
                           :name k
                           :desc (argtypes->insn-desc
                                  (:argtypes v)
                                  (:rettype v))})
                        fn-defs)}
        (insn/visit)
        (insn/write))
    (-> {:name library-symbol
         :flags #{:public}
         :fields [{:name "asPointer"
                   :type IFn
                   :flags #{:public :final}}
                  {:name "asPointerQ"
                   :type IFn
                   :flags #{:public :final}}]
         :methods (vec (concat
                        [{:name :init
                          :desc [String :void]
                          :emit (emit-library-constructor inner-name)}]
                        (mapv (partial emit-wrapped-fn inner-name) fn-defs)))}
        (insn/visit)
        (insn/write))
    {:inner-cls inner-name
     :library library-symbol}))


(def ffi-fns {:library-instance? library-instance?
              :load-library load-library
              :find-symbol find-symbol
              :find-function find-function
              :define-library define-library})
