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

(defn dtype->insn
  [ptr-disposition argtype]
  (case argtype
    :int8 :byte
    :int16 :short
    :int32 :int
    :int64 :long
    :float32 :float
    :float64 :double
    :pointer (case ptr-disposition
               :ptr-as-int (dtype->insn :ptr-as-int (ffi/size-t-type))
               :ptr-as-obj Object
               :ptr-as-ptr tech.v3.datatype.ffi.Pointer
               :ptr-as-platform Pointer)
    :pointer? (case ptr-disposition
               :ptr-as-int (dtype->insn :ptr-as-int (ffi/size-t-type))
               :ptr-as-obj Object
               :ptr-as-ptr tech.v3.datatype.ffi.Pointer
               :ptr-as-platform Pointer)
    :size-t (dtype->insn :ptr-as-int (ffi/size-t-type))
    :void :void
    (do
      (errors/when-not-errorf
       (instance? Class argtype)
       "Argument type %s is unrecognized"
       argtype)
      argtype)))


(defn argtypes->insn-desc
  ([argtypes rettype ptr-disposition]
   (mapv (partial dtype->insn ptr-disposition) (concat argtypes [rettype])))
  ([argtypes rettype]
   (argtypes->insn-desc argtypes rettype :ptr-as-int)))


(defn make-ptr-cast
  [mem-fn-name]
  (concat [[:aload 0]
           [:getfield :this mem-fn-name IFn]
           ;;Swap the IFn 'this' ptr and the argument on the stack
           [:swap]
           [:invokeinterface IFn "invoke" [Object Object]]
           [:checkcast Pointer]]))


(def ptr-cast (make-ptr-cast "asPointer"))
(def ptr?-cast (make-ptr-cast "asPointerQ"))
(def ptr-return
  [[:invokestatic Pointer "nativeValue" [Pointer :long]]
   [:lstore 1]
   [:new tech.v3.datatype.ffi.Pointer]
   [:dup]
   [:lload 1]
   [:invokespecial tech.v3.datatype.ffi.Pointer :init [:long :void]]
   [:areturn]])


(defn ptr-value
  ^Pointer [item]
  (Pointer. (ffi/ptr-value item)))


(defn ptr-value?
  ^Pointer [item]
  (Pointer. (ffi/ptr-value? item)))


(defn emit-library-constructor
  [inner-cls]
  [[:aload 0]
   [:invokespecial :super :init [:void]]
   [:ldc "tech.v3.datatype.ffi.jna"]
   [:ldc "load-library"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:aload 1]
   [:invokevirtual IFn "invoke" [Object Object]]
   [:checkcast NativeLibrary]
   [:ldc inner-cls]
   [:invokestatic com.sun.jna.Native "register" [Class NativeLibrary :void]]
   [:aload 0]
   [:ldc "tech.v3.datatype.ffi.jna"]
   [:ldc "ptr-value"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:putfield :this "asPointer" IFn]
   [:aload 0]
   [:ldc "tech.v3.datatype.ffi.jna"]
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
   :desc (->> (concat (map (partial dtype->insn :ptr-as-obj) (:argtypes fn-def))
                      [(dtype->insn :ptr-as-ptr (:rettype fn-def))])
              (vec))
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
           (argtypes->insn-desc (:argtypes fn-def) (:rettype fn-def) :ptr-as-platform)]
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
  [library-symbol fn-defs {:keys [emit-cls?]
                           :or {emit-cls? true}
                           :as options}]
  ;; First we define the inner class which contains the typesafe static methods
  (let [inner-name (symbol (str library-symbol "$inner"))]
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
    (let [cls-data
          {:name library-symbol
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
                          (mapv (partial emit-wrapped-fn inner-name) fn-defs)))}]
      (when emit-cls?
        (-> cls-data
            (insn/visit)
            (insn/write)))
      {:inner-cls inner-name
       :library library-symbol
       :library-class-def cls-data})))


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library})
