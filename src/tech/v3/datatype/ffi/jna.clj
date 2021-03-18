(ns tech.v3.datatype.ffi.jna
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.ffi.base :as ffi-base]
            [tech.v3.datatype.copy :as dt-copy]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [clojure.tools.logging :as log])
  (:import [com.sun.jna NativeLibrary Pointer Callback CallbackReference]
           [tech.v3.datatype NumericConversions ClojureHelper]
           [tech.v3.datatype.ffi Library]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [clojure.lang IFn RT IPersistentMap IObj MapEntry Keyword IDeref
            ISeq]
           [java.lang.reflect Constructor]
           [java.nio.file Paths]))


(set! *warn-on-reflection* true)


(extend-type Pointer
  ffi/PToPointer
  (convertible-to-pointer? [item] true)
  (->pointer [item] (tech.v3.datatype.ffi.Pointer. (Pointer/nativeValue item)
                                                   {:src-ptr item})))


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
  (Pointer. (ffi-base/ptr-value item)))


(defn ptr-value?
  ^Pointer [item]
  (Pointer. (ffi-base/ptr-value? item)))


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
    [:astore 2]
    [:aload 2]
    [:invokestatic com.sun.jna.Native "register" [Class NativeLibrary :void]]
    [:aload 0]
    [:aload 2]
    [:putfield :this "libraryImpl" NativeLibrary]]
   emit-ptr-ptrq
   [[:aload 0]
    [:dup]
    [:invokevirtual :this "buildFnMap" [Object]]
    [:putfield :this "fnMap" Object]
    [:aload 0]
    [:dup]
    [:invokevirtual :this "buildSymbolTable" [Object]]
    [:putfield :this "symbolTable" Object]
    [:return]]))


(defn find-library-symbol
  ^Pointer [symbol-name symbol-map ^NativeLibrary library]
  (if-let [^Pointer retval (get symbol-map (keyword symbol-name))]
    (tech.v3.datatype.ffi.Pointer/constructNonZero (Pointer/nativeValue retval))
    (do
      (log/warnf "Finding non-predefined symbol \"%s\"" symbol-name)
      (-> (.getGlobalVariableAddress library (str symbol-name))
          (Pointer/nativeValue)
          (tech.v3.datatype.ffi.Pointer/constructNonZero)))))


(defn emit-find-symbol
  []
  [[:ldc "tech.v3.datatype.ffi.jna"]
   [:ldc "find-library-symbol"]
   [:invokestatic ClojureHelper "findFn" [String String IFn]]
   [:aload 1]
   [:aload 0]
   [:getfield :this "symbolTable" Object]
   [:aload 0]
   [:getfield :this "libraryImpl" NativeLibrary]
   [:invokeinterface IFn "invoke" [Object Object Object Object]]
   [:checkcast tech.v3.datatype.ffi.Pointer]
   [:areturn]])


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


(defn emit-constructor-find-symbol
  [symbol-name]
  [[:aload 0]
   [:getfield :this "libraryImpl" NativeLibrary]
   [:ldc (name symbol-name)]
   [:invokevirtual NativeLibrary "getGlobalVariableAddress" [String Pointer]]])


(defn define-jna-library
  [classname fn-defs symbols _options]
  ;; First we define the inner class which contains the typesafe static methods
  (let [inner-name (symbol (str classname "$inner"))]
    [{:name classname
      :flags #{:public}
      :interfaces [IDeref Library]
      :fields [{:name "asPointer"
                :type IFn
                :flags #{:public :final}}
               {:name "asPointerQ"
                :type IFn
                :flags #{:public :final}}
               {:name "libraryImpl"
                :type NativeLibrary
                :flags #{:public :final}}
               {:name "fnMap"
                :type Object
                :flags #{:public :final}}
               {:name "symbolTable"
                :type Object
                :flags #{:public :final}}]
      :methods (vec (concat
                     [{:name :init
                       :desc [String :void]
                       :emit (emit-library-constructor inner-name)}
                      {:name "findSymbol"
                       :desc [String tech.v3.datatype.ffi.Pointer]
                       :emit (emit-find-symbol)}
                      (ffi-base/emit-library-fn-map classname fn-defs)
                      (ffi-base/emit-library-symbol-table classname symbols
                                                          emit-constructor-find-symbol)
                      {:name"deref"
                       :desc [Object]
                       :emit [[:aload 0]
                              [:getfield :this "fnMap" Object]
                              [:areturn]]}]
                     (mapv (partial emit-wrapped-fn inner-name) fn-defs)))}
     {:name inner-name
      :flags #{:public :static}
      :methods (mapv (fn [[k v]]
                       {:flags #{:public :static :native}
                        :name k
                        :desc (argtypes->insn-desc
                               (:argtypes v)
                               (:rettype v)
                               :ptr-as-platform)})
                     fn-defs)}]))


(defn define-library
  [fn-defs symbols
   {:keys [classname] :as options}]
  (let [instantiate? (nil? classname)
        classname (or classname (str "tech.v3.datatype.ffi.jna." (name (gensym))))]
    (ffi-base/define-library fn-defs symbols classname define-jna-library
      instantiate? options)))


(defn platform-ptr->ptr
  [arg-idx]
  [[:aload arg-idx]
   [:invokestatic Pointer "nativeValue" [Pointer :long]]
   [:invokestatic tech.v3.datatype.ffi.Pointer "constructNonZero"
    [:long tech.v3.datatype.ffi.Pointer]]])


(defn define-foreign-interface
  [rettype argtypes options]
  (let [classname (or (:classname options)
                      (symbol (str "tech.v3.datatype.ffi.jna.ffi_" (name (gensym))))) ]
    (ffi-base/define-foreign-interface classname rettype argtypes
      {:src-ns-str "tech.v3.datatype.ffi.jna"
       :platform-ptr->ptr platform-ptr->ptr
       :ptrtype Pointer
       :interfaces [Callback]})))


(defn foreign-interface-instance->c
  [_iface-def inst]
  (let [jna-ptr (CallbackReference/getFunctionPointer inst)]
    ;;make sure we keep the src ptr in scope
    (tech.v3.datatype.ffi.Pointer. (Pointer/nativeValue jna-ptr)
                                   {:src-ptr jna-ptr})))


(def ffi-fns {:load-library load-library
              :find-symbol find-symbol
              :define-library define-library
              :define-foreign-interface define-foreign-interface
              :foreign-interface-instance->c foreign-interface-instance->c})


;;JNA's array->native copy pathway is just a lot faster than unsafe's
;;We know that one of the two buffers is native memory
(defn jna-copy-memory
  [src-buf dst-buf src-dt ^long n-elems]
  (cond
    (instance? ArrayBuffer src-buf)
    (let [^ArrayBuffer src-buf src-buf
          dst-buf (Pointer. (.address ^NativeBuffer dst-buf))]
      (case src-dt
        :int8 (.write dst-buf 0 ^bytes (.ary-data src-buf) (.offset src-buf) n-elems)
        :int16 (.write dst-buf 0 ^shorts (.ary-data src-buf) (.offset src-buf) n-elems)
        :int32 (.write dst-buf 0 ^ints (.ary-data src-buf) (.offset src-buf) n-elems)
        :int64 (.write dst-buf 0 ^longs (.ary-data src-buf) (.offset src-buf) n-elems)
        :float32 (.write dst-buf 0 ^floats (.ary-data src-buf) (.offset src-buf) n-elems)
        :float64 (.write dst-buf 0 ^doubles (.ary-data src-buf) (.offset src-buf) n-elems)
        )
      )
    (instance? ArrayBuffer dst-buf)
    (let [src-buf (Pointer. (.address ^NativeBuffer src-buf))
          ^ArrayBuffer dst-buf dst-buf]
      (case src-dt
        :int8 (.read src-buf 0 ^bytes (.ary-data dst-buf) (.offset dst-buf) n-elems)
        :int16 (.read src-buf 0 ^shorts (.ary-data dst-buf) (.offset dst-buf) n-elems)
        :int32 (.read src-buf 0 ^ints (.ary-data dst-buf) (.offset dst-buf) n-elems)
        :int64 (.read src-buf 0 ^longs (.ary-data dst-buf) (.offset dst-buf) n-elems)
        :float32 (.read src-buf 0 ^floats (.ary-data dst-buf) (.offset dst-buf) n-elems)
        :float64 (.read src-buf 0 ^doubles (.ary-data dst-buf) (.offset dst-buf) n-elems)))
    ;;both native buffers
    :else
    (dt-copy/unsafe-copy-memory src-buf dst-buf src-dt n-elems)))


;; (reset! dt-copy/fast-copy-fn* jna-copy-memory)
