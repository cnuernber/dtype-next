(ns tech.v3.datatype.ffi
  "Generalized C FFI interface that unifies JNA and JDK-16 FFI architectures."
  (:require [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.base :as base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.errors :as errors]
            [clojure.tools.logging :as log])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]
           [java.nio.charset Charset]
           [tech.v3.datatype.ffi Pointer]))


(set! *warn-on-reflection* true)


(def arch64-set #{"x86_64" "amd64"})


(defn size-t-size
  ^long []
  (let [arch (.toLowerCase (System/getProperty "os.arch"))]
    (if (arch64-set arch)
      8
      4)))


(defmacro size-t-compile-time-switch
  "Run either int32 based code or int64 based code depending
   on the runtime size of size-t"
  [int-body long-body]
  (case (size-t-size)
    4 `~int-body
    8 `~long-body))


(defn size-t-type
  []
  (if (= (size-t-size) 8)
    :int64
    :int32))

(casting/alias-datatype! :size-t (size-t-type))

(defn ptr-int-type
  []
  (size-t-type))

(casting/alias-datatype! :pointer (size-t-type))


(defn lower-type
  [argtype]
  (if (= argtype :size-t)
    (size-t-type)
    argtype))


(defn jdk-ffi?
  "Is the JDK foreign function interface enabled?"
  []
  (try
    (boolean (Class/forName "jdk.incubator.foreign.LibraryLookup"))
    (catch Throwable e false)))


(defn jna-ffi?
  "Is JNA's ffi interface enabled?"
  []
  (try
    (boolean (Class/forName "com.sun.jna.Pointer"))
    (catch Throwable e false)))


(defn jdk-mmodel?
  "Is the JDK native memory model enabled?"
  []
  (try
    (boolean (Class/forName "jdk.incubator.foreign.MemoryAddress"))
    (catch Throwable e false)))


(defprotocol PToPointer
  (convertible-to-pointer? [item]
    "Query whether an item is convertible to a pointer.")
  (^Pointer ->pointer [item]
   "Convert an item into a Pointer, throwing an exception upon failure."))


(defn ^:private encoding->info
  [encoding]
  (case (or encoding :utf-8)
    :ascii [1 "ASCII"]
    :utf-8 [1 "UTF-8"]
    :utf-16 [2 "UTF-16"]
    :utf-32 [4 "UTF-32"]))


(defn string->c
  "Convert a java String to a zero-padded c-string.  Available encodings are

  * `:ascii`, `:utf-8` (default), `:utf-16`, and `:utf-32`

  String data will be zero padded."
  ^NativeBuffer [str-data & [encoding]]
  (let [[enc-width enc-name] (encoding->info encoding)
        charset (Charset/forName (str enc-name))
        byte-data (.getBytes (str str-data) charset)
        n-bytes (alength byte-data)
        nbuf (dtype-cmc/make-container :native-heap :int8 {:resource-type :auto}
                                       (+ n-bytes (long enc-width)))]
    (dtype-cmc/copy! byte-data (base/sub-buffer nbuf 0 n-bytes))
    nbuf))


(defn c->string
  "Convert a zero-terminated c-string to a java string.  See documentation for
  `string->c` for encodings."
  ^String [data & [encoding]]
  (let [ptr-data (->pointer data)]
    (if (== 0 (.address ptr-data))
      nil
      (let [nbuf (native-buffer/wrap-address ptr-data Integer/MAX_VALUE :int8
                                             (dtype-proto/platform-endianness)
                                             nil)
            [enc-width enc-name] (encoding->info encoding)
            nbuf (case (long enc-width)
                   1 nbuf
                   2 (native-buffer/set-native-datatype nbuf :int16)
                   4 (native-buffer/set-native-datatype nbuf :int32))
            zero-pad-idx (long (loop [idx 0
                                      finished? (== 0 (long (nbuf 0)))]
                                 (if finished?
                                   idx
                                   (let [nidx (inc idx)]
                                     (recur nidx (== 0 (long (nbuf nidx))))))))
            bbuf (-> (base/sub-buffer nbuf 0 zero-pad-idx)
                     (native-buffer/set-native-datatype :int8)
                     (dtype-cmc/->byte-array))
            charset (Charset/forName (str enc-name))]
        (String. bbuf charset)))))


(extend-protocol PToPointer
  Object
  (convertible-to-pointer? [item] (dtype-proto/convertible-to-native-buffer? item))
  (->pointer [item] (-> (dtype-proto/->native-buffer item)
                        (->pointer)))
  Pointer
  (convertible-to-pointer? [item] true)
  (->pointer [item] item)
  NativeBuffer
  (convertible-to-pointer? [item] true)
  (->pointer [item] (Pointer. (.address item))))


(defn ptr-value
  "Item must not be nil.  A long address is returned."
  ^long [item]
  (cond
    (instance? tech.v3.datatype.ffi.Pointer item)
    (.address ^tech.v3.datatype.ffi.Pointer item)
    (instance? tech.v3.datatype.native_buffer.NativeBuffer item)
    (.address ^tech.v3.datatype.native_buffer.NativeBuffer item)
    :else
    (do
      (errors/when-not-errorf
       (convertible-to-pointer? item)
       "Item %s is not convertible to a C pointer" item)
      (.address ^tech.v3.datatype.ffi.Pointer (->pointer item)))))


(defn ptr-value?
  "Item may be nil in which case 0 is returned."
  ^long [item]
  (if item
    (ptr-value item)
    0))


(def ffi-impl*
  (delay
    (let [ffi-data (when (jdk-ffi?)
                     (try @(requiring-resolve 'tech.v3.datatype.ffi.mmodel/ffi-fns)
                          (catch Throwable e
                            (log/warn e "Failed to require jdk-16+ ffi.  Falling back to JNA") nil)))]
      (if ffi-data
        ffi-data
        (try
          @(requiring-resolve 'tech.v3.datatype.ffi.jna/ffi-fns)
          (catch Throwable e
            (throw (Exception. "Neither jdk-16 nor JNA are found on classpath.  FFI functionality is missing."))))))))


(defn load-library
  "Load a library returning an implementation specific library instance.

  Exception if the library cannot be found."
  [libname]
  ((:load-library @ffi-impl*) libname))


(defn find-symbol
  "Find a symbol in a library.  A library symbol is guaranteed to have a conversion to
  a pointer."
  ([libname symbol-name]
   ((:find-symbol @ffi-impl*) libname symbol-name))
  ([symbol-name]
   ((:find-symbol @ffi-impl*) nil symbol-name)))


(defn define-library
  "Define a library.  After this, it is legal to 'import' the symbol.  The library
  class file will be written out to *compile-path* - see documentation for
  insn.core/write.

  * fn-defs - map of fn-name -> {:rettype :argtypes} - see 'find-function'.

  After this functionc call, if output-path is on the classpath then you can
  'import' the defined library.  It's constructor takes a string which is the
  path of the library to bind to (or nil for current process) and it will have
  a correctly-typed function defined for every key in fn-defs.

  Example:

```clojure
user> (dtype-ffi/define-library 'tech.libmemset {:memset {:rettype :pointer
                                                          :argtypes [:pointer :int32 :size-t]}})
{:library tech.libmemset}
user> (import 'tech.libmemset)
tech.libmemset
user> (def inst (tech.libmemset. nil)) ;;nil for current process
#'user/inst
user> (def test-buf (dtype/make-container :native-heap :float32 (range 10)))
#'user/test-buf
user> test-buf
#native-buffer@0x00007F4E28CE56D0<float32>[10]
[0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000, ]
user> (.memset ^tech.libmemset inst test-buf 0 40)
#object[tech.v3.datatype.ffi.Pointer 0x33013c57 \"{:address 0x00007F4E28CE56D0 }\"]
user> test-buf
#native-buffer@0x00007F4E28CE56D0<float32>[10]
[0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, ]
user>
```"
  [library-symbol fn-defs & [options]]
  ((:define-library @ffi-impl*) library-symbol fn-defs options))


(defn find-function
  "Find the symbol and return an IFn implementation wrapping typesafe access
  to the function.  The function's signature has to be completely specified.
  Supported datatypes are:

  * `:void` - return type only.
  * `:int8` `:int16` `:int32` `:int64`
  * `:float32` `:float64`
  * `:size-t` - int32 or int64 depending on cpu architecture.
  * `:pointer` `:pointer?` - Something convertible to a Pointer type.  Potentially
     exception when nil.

  This object's ->pointer method returns the actual function pointer."
  [libname symbol-name rettype & [argtypes options]]
  (let [lib-symbol (:library (define-library
                               (symbol (str "tech.v3.datatype.ffi.Invoker_"
                                            (name symbol-name)
                                            "_"
                                            (name (gensym))))
                               {symbol-name {:rettype rettype
                                             :argtypes argtypes}}
                               options))
        eval-data
        (list 'do (list 'import lib-symbol)
              (list 'let ['libinst (list 'new lib-symbol libname)]
                    (list 'fn (vec (map (fn[idx]
                                          (symbol (str "arg_" idx)))
                                        (range (count argtypes))))
                          (concat
                           [(symbol (str "." (name symbol-name))) 'libinst]
                           (->> argtypes
                                (map-indexed
                                 (fn [idx argtype]
                                   (let [arg-sym (symbol (str "arg_" idx))]
                                     (case (lower-type argtype)
                                       :int8 (list 'unchecked-byte arg-sym)
                                       :int16 (list 'unchecked-short arg-sym)
                                       :int32 (list 'unchecked-int arg-sym)
                                       :int64 (list 'unchecked-long arg-sym)
                                       :float32 (list 'unchecked-float arg-sym)
                                       :float64 (list 'unchecked-double arg-sym)
                                       :pointer arg-sym
                                       :pointer? arg-sym)))))))))]
    (eval eval-data)))


(defn define-foreign-interface
  "Define a strongly typed interface that can then be used with fn->c."
  [iface-symbol rettype argtypes & [options]]
  ((:define-foreign-interface @ffi-impl*) iface-symbol rettype argtypes options))


(defn fn->c
  "Convert something implementing the Clojure IFn interface to a Pointer value.
  Callers must specify the C-interface they expect to be used.  See find-function
  for the definition of rettype and argtypes.  Returns a Pointer suitable to be
  passed to C functions as a callback with the given signature."
  ^Pointer [ifn iface & [options]]
  ((:fn->pointer @ffi-impl*) ifn iface options))
