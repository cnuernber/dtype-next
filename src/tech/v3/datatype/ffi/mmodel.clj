(ns tech.v3.datatype.ffi.mmodel
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.ffi :as ffi]
            [insn.core :as insn]
            [clojure.string :as s]
            [clojure.java.io :as io])
  (:import [jdk.incubator.foreign LibraryLookup CLinker FunctionDescriptor
            MemoryLayout LibraryLookup$Symbol]
           [jdk.incubator.foreign MemoryAddress Addressable MemoryLayout]
           [java.lang.invoke MethodHandle MethodType]
           [java.nio.file Path Paths]
           [java.lang.reflect Constructor]
           [tech.v3.datatype NumericConversions ClojureHelper]
           [tech.v3.datatype.ffi Pointer]
           [clojure.lang IFn RT ISeq Var Keyword]))


(set! *warn-on-reflection* true)

;;  https://openjdk.java.net/projects/jdk/16/
;;  https://download.java.net/java/early_access/jdk16/docs/api/jdk.incubator.foreign/jdk/incubator/foreign/CLinker.html
;;  https://openjdk.java.net/jeps/389
;;  https://github.com/cs-activeviam-simd/vector-profiling/blob/master/src/test/java/fr/centralesupelec/simd/VectorOffHeapTest.java


;; MethodHandle strlen = CLinker.getInstance().downcallHandle(
;;         LibraryLookup.ofDefault().lookup("strlen"),
;;         MethodType.methodType(long.class, MemoryAddress.class),
;;         FunctionDescriptor.of(C_LONG, C_POINTER)
;;     );


(defn ptr-value
  ^MemoryAddress [item]
  (MemoryAddress/ofLong (ffi/ptr-value item)))


(defn ptr-value?
  ^MemoryAddress [item]
  (MemoryAddress/ofLong (ffi/ptr-value? item)))


(extend-protocol ffi/PToPointer
  MemoryAddress
  (is-convertible-to-pointer? [item] true)
  (->pointer [item] (Pointer. (.toRawLongValue item)))
  Addressable
  (is-convertible-to-pointer? [item] true)
  (->pointer [item] (Pointer. (-> (.address item)
                                  (.toRawLongValue)))))


(defn ->path
  ^Path [str-base & args]
  (Paths/get (str str-base) (into-array String args)))


(defn load-library
  ^LibraryLookup [libname]
  (cond
    (instance? LibraryLookup libname)
    libname
    (instance? Path libname)
    (LibraryLookup/ofPath libname)
    (string? libname)
    (LibraryLookup/ofLibrary libname)
    (nil? libname)
    (LibraryLookup/ofDefault)
    :else
    (errors/throwf "Unrecognized libname type %s" (type libname))))


(defn find-symbol
  (^LibraryLookup$Symbol [libname symbol-name]
   (let [symbol-name (cond
                       (symbol? symbol-name)
                       (name symbol-name)
                       (keyword? symbol-name)
                       (name symbol-name)
                       :else
                       (str symbol-name))]
     (-> (load-library libname)
         (.lookup symbol-name)
         (.get))))
  (^LibraryLookup$Symbol [symbol-name]
   (find-symbol nil symbol-name)))


(defn memory-layout-array
  ^"[Ljdk.incubator.foreign.MemoryLayout;" [& args]
  (into-array MemoryLayout args))


(defn argtype->mem-layout-type
  [argtype]
  (case (ffi/lower-type argtype)
    :int8 CLinker/C_CHAR
    :int16 CLinker/C_SHORT
    :int32 CLinker/C_INT
    :int64 CLinker/C_LONG
    :float32 CLinker/C_FLOAT
    :float64 CLinker/C_DOUBLE
    :pointer? CLinker/C_POINTER
    :pointer CLinker/C_POINTER))


(defn sig->fdesc
  ^FunctionDescriptor [{:keys [rettype argtypes]}]
  (if (or (= :void rettype)
          (nil? rettype))
    (FunctionDescriptor/ofVoid (->> argtypes
                                    (map argtype->mem-layout-type)
                                    (apply memory-layout-array)))
    (FunctionDescriptor/of (argtype->mem-layout-type rettype)
                           (->> argtypes
                                (map argtype->mem-layout-type)
                                (apply memory-layout-array)))))


(defn argtype->cls
  ^Class [argtype]
  (case (ffi/lower-type argtype)
    :int8 Byte/TYPE
    :int16 Short/TYPE
    :int32 Integer/TYPE
    :int64 Long/TYPE
    :float32 Float/TYPE
    :float64 Double/TYPE
    :pointer MemoryAddress
    :pointer? MemoryAddress
    :string MemoryAddress))


(defn sig->method-type
  ^MethodType [{:keys [rettype argtypes]}]
  (let [^"[Ljava.lang.Class;" cls-ary (->> argtypes
                                           (map argtype->cls)
                                           (into-array Class))]
    (MethodType/methodType (argtype->cls rettype) cls-ary)))


(defn sig->cls-name
  [{:keys [rettype argtypes]}]
  (->> (concat [rettype] argtypes)
       (map (fn [argtype]
              (if-not (or (= :void argtype )
                          (nil? argtype))
                (case argtype
                  :int8 "B"
                  :int16 "S"
                  :int32 "I"
                  :int64 "J"
                  :float32 "F"
                  :float64 "D"
                  "P")
                "V")))
       (s/join "_")))


(defn argtype->insn-type
  ([ptr-type argtype]
   (if (or (= :void argtype)
           (nil? argtype))
     :void
     (case (ffi/lower-type argtype)
       :int8 :byte
       :int16 :short
       :int32 :int
       :int64 :long
       :float32 :float
       :float64 :double
       :pointer (case ptr-type
                  :ptr-as-platform MemoryAddress
                  :ptr-as-ptr Pointer
                  :ptr-as-obj Object)
       :pointer? (case ptr-type
                   :ptr-to-platform MemoryAddress
                   :ptr-to-ptr Pointer
                   :ptr-as-obj Object))))
  ([argtype]
   (argtype->insn-type :ptr-to-platform argtype)))


(defn library-sym->method-handle
  ^MethodHandle [library symbol-name rettype & argtypes]
  (let [sym (find-symbol library symbol-name)
        sig {:rettype rettype
             :argtypes argtypes}
        fndesc (sig->fdesc sig)
        methoddesc (sig->method-type sig)
        linker (CLinker/getInstance)]
    (.downcallHandle linker sym methoddesc fndesc)))


(defn emit-lib-constructor
  [fn-defs]
  (->>
   (concat
    [[:aload 0]
     [:invokespecial :super :init [:void]]
     [:aload 0]
     [:ldc "tech.v3.datatype.ffi.mmodel"]
     [:ldc "ptr-value"]
     [:invokestatic ClojureHelper "findFn" [String String IFn]]
     [:putfield :this "asPointer" IFn]
     [:aload 0]
     [:ldc "tech.v3.datatype.ffi.mmodel"]
     [:ldc "ptr-value?"]
     [:invokestatic ClojureHelper "findFn" [String String IFn]]
     [:putfield :this "asPointer" IFn]
     [:ldc "tech.v3.datatype.ffi.mmodel"]
     [:ldc "library-sym->method-handle"]
     [:invokestatic ClojureHelper "findFn" [String String IFn]]
     ;;1 is the string constructor argument
     ;;2 is the method handle resolution system
     [:astore 2]]
    (mapcat
     (fn [[fn-name {:keys [rettype argtypes]}]]
       (let [hdl-name (str (name fn-name) "_hdl")]
         (concat
          [[:aload 0] ;;this-ptr
           [:aload 2] ;;handle resolution fn
           [:aload 1] ;;libname
           [:ldc (name fn-name)]
           [:ldc (name rettype)]
           [:invokestatic Keyword "intern" [String Keyword]]]
          (mapcat (fn [argtype]
                    [[:ldc (name argtype)]
                     [:invokestatic Keyword "intern" [String Keyword]]])
                  argtypes)
          [[:invokeinterface IFn "invoke"
            (concat [Object Object Object] (repeat (inc (count argtypes)) Object))]
           [:checkcast MethodHandle]
           [:putfield :this hdl-name MethodHandle]])))
     fn-defs)
    [[:return]])
   (vec)))

(defn make-ptr-cast
  [cast-fn]
  [[:aload 0]
   [:getfield :this cast-fn IFn]
   [:swap]
   [:invokeinterface IFn "invoke" [Object Object]]
   [:checkcast MemoryAddress]])


(def ptr-cast (make-ptr-cast "asPointer"))
(def ptr?-cast (make-ptr-cast "asPointerQ"))
(def ptr-return
  [[:invokeinterface MemoryAddress "toRawLongValue" [:long]]
   [:lstore 1]
   [:new Pointer]
   [:dup]
   [:lload 1]
   [:invokespecial Pointer :init [:long :void]]
   [:areturn]])


(defn emit-fn-def
  [hdl-name rettype argtypes]
  (->> (concat
        [[:aload 0]
         [:getfield :this hdl-name MethodHandle]]
        (-> (reduce
             (fn [[retval offset] argtype]
               (let [argtype (ffi/lower-type argtype)]
                 [(concat
                   retval
                   (case argtype
                     :int8 [[:iload offset]]
                     :int16 [[:iload offset]]
                     :int32 [[:iload offset]]
                     :int64 [[:lload offset]]
                     :float32 [[:fload offset]]
                     :float64 [[:dload offset]]
                     :pointer (concat [[:aload offset]]
                                      ptr-cast)
                     :pointer? (concat [[:aload offset]]
                                       ptr?-cast)))
                  (+ (long offset)
                     (long (case argtype
                             :int64 2
                             :float64 2
                             1)))]))
             ;;Start at 1 because 0 is the 'this' object
             [[] 1]
             argtypes)
            (first))
        [[:invokevirtual MethodHandle "invokeExact"
          (concat (map (partial argtype->insn-type :ptr-as-platform) argtypes)
                  [(argtype->insn-type :ptr-as-platform rettype)])]]
        (case (ffi/lower-type rettype)
          :int8 [[:ireturn]]
          :int16 [[:ireturn]]
          :int32 [[:ireturn]]
          :int64 [[:lreturn]]
          :float32 [[:freturn]]
          :float64 [[:dreturn]]
          :pointer ptr-return
          :pointer? ptr-return))
       (vec)))


(defn base-define-library
  [library-symbol fn-defs options]
  {:name library-symbol
   :flags #{:public}
   :fields
   (->> (concat
         [{:name "asPointer"
           :type IFn
           :flags #{:public :final}}
          {:name "asPointerQ"
           :type IFn
           :flags #{:public :final}}]
         (map (fn [[fn-name _fn-args]]
                {:name (str (name fn-name) "_hdl")
                 :type MethodHandle
                 :flags #{:public :final}})
              fn-defs))
        (vec))
   :methods
   (->> (concat
         [{:name :init
           :flags #{:public}
           :desc [String :void]
           :emit (emit-lib-constructor fn-defs)}]
         (map
          (fn [[fn-name fn-data]]
            (let [hdl-name (str (name fn-name) "_hdl")
                  {:keys [rettype argtypes]} fn-data]
              {:name fn-name
               :flags #{:public}
               :desc (concat (map (partial argtype->insn-type :ptr-as-obj)
                                  argtypes)
                             [(argtype->insn-type :ptr-as-ptr rettype)])
               :emit (emit-fn-def hdl-name rettype argtypes)}))
          fn-defs))
        (vec))})


(defn define-library
  [library-symbol fn-defs options]
  (-> (base-define-library library-symbol fn-defs options)
      (insn/visit)
      (insn/write))
  {:library library-symbol})


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library})
