(ns tech.v3.datatype.ffi.jna
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.errors :as errors]
            [insn.core :as insn])
  (:import [com.sun.jna NativeLibrary Pointer]
           [tech.v3.datatype NumericConversions]
           [clojure.lang IFn RT]
           [java.lang.reflect Constructor]))


(set! *warn-on-reflection* true)


(extend-type Pointer
  ffi/PToPointer
  (convertible-to-pointer? [item] true)
  (->pointer [item] (ffi/->Pointer (Pointer/nativeValue item))))


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
  (mapv dtype->insn
        (concat argtypes [rettype])))

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
  (let [cls-name (str "Invoker_" (name (gensym)))]
    {:name (symbol (str "tech.v3.datatype.ffi.jna." cls-name))
     :interfaces [IFn]
     :fields [{:flags #{:public :final}
               :name "asPointer"
               :type IFn}
              {:flags #{:public :final}
               :name "asPointerQ"
               :type IFn}]
     :methods [{:flags #{:public}
                :name :init
                :desc [IFn IFn :void]
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
                       [:return]]}
               {:flags #{:public :static :native}
                :name symbol-name
                :desc (argtypes->insn-desc argtypes rettype)}
               {:flags #{:public}
                :name :invoke
                :desc (vec (repeat (inc (count argtypes)) Object))
                :emit (emit-invoke cls-name symbol-name argtypes rettype)}]}))


(defn ptr-value
  ^long [item]
  (.address ^tech.v3.datatype.ffi.Pointer (ffi/->pointer item)))


(defn ptr-value?
  ^long [item]
  (if item
    (do
      (errors/when-not-errorf
       (ffi/convertible-to-pointer? item)
       "Item %s is not convertible to a C pointer" item)
      (.address ^tech.v3.datatype.ffi.Pointer (ffi/->pointer item)))
    (long 0)))


(defn cls->inst
  [invoker-cls]
  (let [^Constructor ctor (first (.getDeclaredConstructors ^Class invoker-cls))]
    (.newInstance ctor (object-array [ptr-value ptr-value?]))))


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


(def ffi-fns {:library-instance? library-instance?
              :load-library load-library
              :find-symbol find-symbol
              :find-function find-function})
