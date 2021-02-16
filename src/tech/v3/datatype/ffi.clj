(ns tech.v3.datatype.ffi
  "Generalized C FFI interface that unifies JNA and JDK-16 FFI architectures."
  (:require [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.base :as base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [clojure.tools.logging :as log])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype.ffi Pointer]
           [java.nio.charset Charset]
           [java.lang.reflect Constructor]
           [clojure.lang IFn]))


(set! *warn-on-reflection* true)


(def arch64-set #{"x86_64" "amd64"})


(defn size-t-size
  "Get the size in bytes of a size-t integer."
  ^long []
  (let [arch (.toLowerCase (System/getProperty "os.arch"))]
    (if (arch64-set arch)
      8
      4)))


(defmacro ^:no-doc size-t-compile-time-switch
  "Run either int32 based code or int64 based code depending
   on the runtime size of size-t.  Use with extreme caution - this means that
  size-t size will be set upon compilation of your code which may be on a different
  platform than where your code runs."
  [int-body long-body]
  (case (size-t-size)
    4 `~int-body
    8 `~long-body))


(defn size-t-type
  "the size-t datatype - either `:int32` or `:int64`."
  []
  (if (= (size-t-size) 8)
    :int64
    :int32))


(defn ptr-int-type
  "Get the integer type of a pointer - either `:int32` or `:int64`."
  []
  (size-t-type))


(defn lower-type
  "Downcast `:size-t` to its integer equivalent."
  [argtype]
  (if (= argtype :size-t)
    (size-t-type)
    argtype))


(defn jdk-ffi?
  "Is the JDK foreign function interface available?"
  []
  (try
    (boolean (Class/forName "jdk.incubator.foreign.LibraryLookup"))
    (catch Throwable e false)))


(defn jna-ffi?
  "Is JNA's ffi interface available?"
  []
  (try
    (boolean (Class/forName "com.sun.jna.Pointer"))
    (catch Throwable e false)))


(defn jdk-mmodel?
  "Is the JDK native memory model available?"
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


(defn instantiate-class
  "Utility function to instantiate a class via its first constructor in its list
  of declared constructors.  Works with classes returned from 'define-library'
  and 'define-foreign-interface'.  Uses reflection."
  [cls & args]
  (let [^Constructor constructor (-> (.getDeclaredConstructors ^Class cls)
                                     (first))]
    (.newInstance constructor (object-array args))))


(def ^:private ffi-impl* (atom nil))


(defn set-ffi-impl!
  "Set the global ffi implementation.  Options are
  * :jdk - namespace `'tech.v3.datatype.ffi.mmodel/ffi-fns` - only available with jdk's foreign function module enabled.
  * :jna - namespace `''tech.v3.datatype.ffi.jna/ffi-fns` - available if JNA version 5+ is in the classpath."
  [ffi-kwd]
  (case ffi-kwd
    :jdk (reset! ffi-impl* @(requiring-resolve 'tech.v3.datatype.ffi.mmodel/ffi-fns))
    :jna (reset! ffi-impl* @(requiring-resolve 'tech.v3.datatype.ffi.jna/ffi-fns))))


(defn- ffi-impl
  "Get an implementation of the actual FFI interface.  This is for internal use only."
  []
  (when (nil? @ffi-impl*)
    ;;prefer JDK support
    (try
      (set-ffi-impl! :jdk)
      (catch Throwable e
        (log/debug e "Failed to load JDK FFI implementation.")
        (try
          (set-ffi-impl! :jna)
          (catch Throwable e
            (reset! ffi-impl* :failed)
            (log/error e "Failed to find a suitable ffi implementation.
Attempted both :jdk and :jna -- call set-ffi-impl! from the repl to see specific failure."))))))
  @ffi-impl*)



(defn load-library
  "Load a library returning an implementation specific library instance.

  Exception if the library cannot be found."
  [libname]
  ((:load-library (ffi-impl)) libname))


(defn find-symbol
  "Find a symbol in a library.  A library symbol is guaranteed to have a conversion to
  a pointer."
  ([libname symbol-name]
   ((:find-symbol (ffi-impl)) libname symbol-name))
  ([symbol-name]
   ((:find-symbol (ffi-impl)) nil symbol-name)))


(defn define-library
  "Define a library returning the class.  The class can be instantiated with a string
  naming the library path or nil for the current process.  After instantiation the
  library instance will have strongly typed methods named the same as the
  keyword keys in fn-defs efficiently bound to symbols of the same exact name in
  library.

  * `fn-defs` - map of fn-name -> {:rettype :argtypes}.
     * `argtypes` -
       * `:void` - return type only.
       * `:int8` `:int16` `:int32` `:int64`
       * `:float32` `:float64`
       * `:size-t` - int32 or int64 depending on cpu architecture.
       * `:pointer` `:pointer?` - Something convertible to a Pointer type.  Potentially
          exception when nil.
     * `rettype` - any argtype plus :void

  Options:

  * `:classname` - If classname (a symbol) is provided a .class file is saved to
  *compile-path* after which `(import classname)` will be a validcall meaning that
  after AOT no further reflection or class generation is required to access the
  class explicitly.  That being said 'import' does not reload classes that are
  already on the classpath so this option is best used after library has stopped
  changing.

  Example:

```clojure
user> (dtype-ffi/define-library {:memset {:rettype :pointer
                                          :argtypes [:pointer :int32 :size-t]}}
                                {:classname 'tech.libmemset})
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
  [fn-defs & [options]]
  ((:define-library (ffi-impl)) fn-defs options))


(defn instantiate-library
  "Uses reflection to instantiate a library"
  [library-def libpath]
  (instantiate-class (:library-class library-def) libpath))


(defn find-function
  "Find the symbol and return an IFn implementation wrapping typesafe access
  to the function.  The function's signature has to be completely specified.
  Supported datatypes are

  This object's ->pointer protocol implementation returns the actual function
  pointer.  This function uses 'eval' and thus also uses reflection."
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
  "Define a strongly typed instance that can be used with
  foreign-interface-instance->c.  This instance takes a single constructor argument
  which is the IFn that it is wrapping.  It has a single typesafe 'invoke' method
  taking types defined by the underlying binding and transforming them into types
  that Clojure expects - for instance pointers are transformed to/from
  `tech.v3.datatype.ffi.Pointer` upon entry/exit from the wrapped IFn.

  * `rettype` - The return type of the function.
  * `argtypes` - A possibly empty sequence of argument types.

  Options:

  * `:classname` - Similar to `:classname` in 'define-library'.  A class will be
     generated to *compile-path* and after this statement `(import classname)` will
     be a valid call.

  See foreign-interface-instance->c for example."
  [rettype argtypes & [{:as options}]]
  ((:define-foreign-interface (ffi-impl)) rettype argtypes options))


(defn instantiate-foreign-interface
  [ffi-def ifn]
  (errors/when-not-errorf
   (instance? IFn ifn)
   "IFn argument (%s) must be an instance of IFn" ifn)
  (instantiate-class (:foreign-iface-class ffi-def) ifn))


(defn foreign-interface-instance->c
  "Convert an instance of the above foreign interface definition into a Pointer
  suitable to be called from C.  Callers must ensure that foreign-inst is visible
  to the gc the entire time the foreign system has a reference to the pointer.

  * `foreign-inst` - an instance of the class defined by 'define-foreign-interface'."
  ^Pointer [ffi-def foreign-inst]
  ((:foreign-interface-instance->c @ffi-impl*) ffi-def foreign-inst))
