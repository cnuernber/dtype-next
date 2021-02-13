(ns tech.v3.datatype.ffi.base
  (:require [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi :as ffi])
  (:import [clojure.lang IFn RT]
           [tech.v3.datatype ClojureHelper]))


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
    :void [:return]
    :int8 [:ireturn]
    :int16 [:ireturn]
    :int32 [:ireturn]
    :int64 [:lreturn]
    :pointer ptr-return
    :float32 [:freturn]
    :float64 [:dreturn]))


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
   [[:invokevirtual IFn "invoke" (repeat (inc (count argtypes)) Object)]]
   (case (ffi/lower-type rettype)
     :int8 [[:checkcast Byte]
            [:invokevirtual Byte "asByte" [Byte :byte]]
            [:ireturn]]
     :int16 [[:checkcast Short]
             [:invokevirtual Short "asShort" [Short :short]]
             [:ireturn]]
     :int32 [[:checkcast Integer]
             [:invokevirtual Integer "asInt" [Short :int]]
             [:ireturn]]
     :int64 [[:checkcast Long]
             [:invokevirtual Long "asLong" [Long :long]]
             [:lreturn]]
     :float32 [[:checkcast Float]
               [:invokevirtual Float "asFloat" [Float :float]]
               [:freturn]]
     :float64 [[:checkcast Double]
               [:invokevirtual Double "asDouble" [Double :double]]
               [:dreturn]]
     :pointer (ifn-return-ptr "asPointer")
     :pointer? (ifn-return-ptr "asPointerQ"))))
