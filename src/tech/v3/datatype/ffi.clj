(ns tech.v3.datatype.ffi
  "Generalized C Foreign Function Interface (FFI) that unifies JNA and JDK-16 FFI architectures.

  Users can dynamically define and load libraries and define callbacks that C can then
  call.

  This namespace is meant to work with the `struct` namespace where you can define
  C-structs laid out precisely in memory.

  Available datatypes for the binding layer:

  * `:int8` `:int16` `:int32` `:int64` `:float32` `:float64` `:pointer` `:pointer?`
    `:size-t`.

  `:pointer`, `:pointer?`, `:size-t` are all types that change their underlying
  definition depending on if the system is 32 bit or 64 bit.

  Note that unsigned types will work but the interface will have to be defined in
  terms of their signed analogues.

Example:

```clojure

user> (require '[tech.v3.datatype :as dtype])
nil
user> (require '[tech.v3.datatype.ffi :as dtype-ffi])
nil
user> (def libmem-def (dtype-ffi/define-library
                        {:qsort {:rettype :void
                                 :argtypes [['data :pointer]
                                            ['nitems :size-t]
                                            ['item-size :size-t]
                                            ['comparator :pointer]]}}))
#'user/libmem-def
user> (def libmem-inst (dtype-ffi/instantiate-library libmem-def nil))
#'user/libmem-inst
user> (def         libmem-fns @libmem-inst)
#'user/libmem-fns
user> (def         qsort (:qsort libmem-fns))
#'user/qsort
user> (def comp-iface-def (dtype-ffi/define-foreign-interface :int32 [:pointer :pointer]))
#'user/comp-iface-def
user> (require '[tech.v3.datatype.native-buffer :as native-buffer])
nil
user> (def comp-iface-inst (dtype-ffi/instantiate-foreign-interface
                         comp-iface-def
                         (fn [^tech.v3.datatype.ffi.Pointer lhs ^tech.v3.datatype.ffi.Pointer rhs]
                           (let [lhs (.getDouble (native-buffer/unsafe) (.address lhs))
                                 rhs (.getDouble (native-buffer/unsafe) (.address rhs))]
                             (Double/compare lhs rhs)))))
#'user/comp-iface-inst
user> (def comp-iface-ptr (dtype-ffi/foreign-interface-instance->c
                        comp-iface-def
                        comp-iface-inst))
#'user/comp-iface-ptr
user> (def dbuf (dtype/make-container :native-heap :float64 (shuffle (range 100))))
09:00:27.900 [tech.resource.gc ref thread] INFO  tech.v3.resource.gc - Reference thread starting
#'user/dbuf
user> dbuf
#native-buffer@0x00007F47084E4210<float64>[100]
[80.00, 90.00, 96.00, 29.00, 19.00, 12.00, 88.00, 94.00, 81.00, 17.00, 54.00, 52.00, 64.00, 86.00, 10.00, 76.00, 49.00, 5.000, 32.00, 69.00, ...]
user> (qsort dbuf (dtype/ecount dbuf) Double/BYTES comp-iface-ptr)
nil
user> dbuf
#native-buffer@0x00007F47084E4210<float64>[100]
[0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000, 10.00, 11.00, 12.00, 13.00, 14.00, 15.00, 16.00, 17.00, 18.00, 19.00, ...]
```"
  (:require [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.ffi.size-t :as ffi-size-t]
            [tech.v3.datatype.struct :as dt-struct]
            [tech.v3.datatype.dechunk-map :refer [dechunk-map]]
            [tech.v3.datatype.graal-native :as graal-native]
            [tech.v3.resource :as resource]
            [clojure.tools.logging :as log]
            [clojure.string :as s])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype.ffi Pointer Library]
           [java.util.concurrent ConcurrentHashMap]
           [java.util.function BiFunction]
           [java.util Map]
           [java.nio.charset Charset]
           [java.lang.reflect Constructor]
           [java.io File]
           [java.nio.file Paths]
           [clojure.lang IFn IDeref]))


(set! *warn-on-reflection* true)


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
  (^tech.v3.datatype.ffi.Pointer ->pointer [item]
   "Convert an item into a Pointer, throwing an exception upon failure."))


(defn utf16-platform-encoding
  "Get the utf-16 encoding that matches the current platform so you can encode
  a utf-16 string without a bom."
  []
  (case (dtype-proto/platform-endianness)
    :little-endian :utf-16LE
    :big-endian :utf-16BE))


(defn utf32-platform-encoding
  "Get the utf-16 encoding that matches the current platform so you can encode
  a utf-16 string without a bom."
  []
  (case (dtype-proto/platform-endianness)
    :little-endian :utf-32LE
    :big-endian :utf-32BE))


(defn windows-platform?
  []
  (-> (System/getProperty "os.name")
      (.toLowerCase)
      (.contains "win")))


(defn ^:private encoding->info
  [encoding]
  (case (or encoding :utf-8)
    :ascii [1 "ASCII"]
    :utf-8 [1 "UTF-8"]
    :utf-16 [2 "UTF-16"]
    :utf-16LE [2 "UTF-16LE"]
    :utf-16BE [2 "UTF-16BE"]
    :utf-32 [4 "UTF-32"]
    :utf-32LE [4 "UTF-32LE"]
    :utf-32BE [4 "UTF-32BE"]
    :wchar-t (if (windows-platform?)
               (encoding->info (utf16-platform-encoding))
               (encoding->info (utf32-platform-encoding)))))

(def ^{:tag BiFunction
       :private true} incrementor
  (reify BiFunction
    (apply [this k v] (if-not v 1 (+ 1 (long v))))))

(defonce ^:private string->c-access-map* (atom nil))

(defn- s->c-amap
  ^ConcurrentHashMap [] @string->c-access-map*)


(defn enable-string->c-stats!
  "Enable tracking how often string->c is getting called."
  ([enabled?]
   (reset! string->c-access-map*
           (when enabled? (ConcurrentHashMap.))))
  ([] (enable-string->c-stats! true)))


(defn clear-string->c-stats!
  []
  (when-let [amap (s->c-amap)]
    (.clear amap)))


(defn string->c-data-histogram
  []
  (->> (s->c-amap)
       (into {})
       (sort-by second)
       (mapv vec)))


(defn string->c
  "Convert a java String to a zero-padded c-string.  Available encodings are

  * `:ascii`, `:utf-8` (default), `:utf-16`, `:utf-16LE`, `utf-16-BE` and `:utf-32`

  String data will be zero padded."
  ^NativeBuffer [str-data & [{:keys [encoding]
                              :as options}]]
  (errors/when-not-errorf
   (instance? String str-data)
   "string->c called with object that is not a string: %s" str-data)
  (when-let [amap (s->c-amap)] (.compute amap str-data incrementor))
  (let [[enc-width enc-name] (encoding->info encoding)
        charset (Charset/forName (str enc-name))
        byte-data (.getBytes (str str-data) charset)
        n-bytes (alength byte-data)
        nbuf (dtype-cmc/make-container :native-heap :int8
                                       (merge {:resource-type :auto}
                                              options)
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
      (let [nbuf (native-buffer/wrap-address
                  (.address ptr-data) Integer/MAX_VALUE :int8
                  (dtype-proto/platform-endianness) nil)
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
  ;;Keep a reference to the native buffer so that it doesn't get GC'd while
  ;;this pointer is in scope.
  (->pointer [item] (Pointer. (.address item) {:src-buffer item})))


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
      (set-ffi-impl! :jna)
      (catch Throwable e
        ;;brief error report on this log.
        (log/debugf "Failed to load JNA FFI implementation: %s" e)
        (try
          (set-ffi-impl! :jdk)
          (catch Throwable e
            (reset! ffi-impl* :failed)
            (log/error e "Failed to find a suitable FFI implementation.
Attempted both :jdk and :jna -- call set-ffi-impl! from the repl to see specific failure."))))))
  @ffi-impl*)


(defn find-library
  "This method is useful to check if loading a library will work.  If successful,
  returns the library name that did finally succeed.  This may be different from
  `libname` in the case where java.library.path has been used to actively find the
  library."
  [libname]
  (->> (concat [libname]
               (->> (s/split (System/getProperty "java.library.path") #":")
                    (map (comp str #(Paths/get % (into-array String
                                                             [(System/mapLibraryName libname)]))))
                    (filter #(.exists (File. (str %))))))
       (dechunk-map identity)
       (filter #(try ((:load-library (ffi-impl)) %)
                     true
                     (catch Throwable e false)))
       (first)))


(defn library-loadable?
  [libname]
  (boolean (find-library libname)))


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
  ([fn-defs symbols options]
   ((:define-library (ffi-impl)) fn-defs symbols options))
  ([fn-defs options]
   ((:define-library (ffi-impl)) fn-defs nil options))
  ([fn-defs]
   ((:define-library (ffi-impl)) fn-defs nil nil)))


(defn instantiate-library
  "Uses reflection to instantiate a library"
  ^Library [library-def libpath]
  (instantiate-class (:library-class library-def) libpath))


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
  "Instantiate a foreign interface defintion.  This returns an instance object which
  can then be converted into a c-pointer via `foreign-interface-instance->c`."
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


(defn make-ptr
  "Make an object convertible to a pointer that points to  single value of type
  `dtype`."
  (^NativeBuffer [dtype prim-init-value options]
   (let [dtype (ffi-size-t/lower-ptr-type dtype)
         ^NativeBuffer nbuf (-> (native-buffer/malloc
                                 (casting/numeric-byte-width dtype)
                                 options)
                                (native-buffer/set-native-datatype dtype))
         addr (.address nbuf)
         unsafe (native-buffer/unsafe)]
     (case dtype
       :int8 (.putByte unsafe addr (unchecked-byte prim-init-value))
       :int16 (.putShort unsafe addr (unchecked-short prim-init-value))
       :int32 (.putInt unsafe addr (unchecked-int prim-init-value))
       :int64 (.putLong unsafe addr (unchecked-long prim-init-value))
       :float32 (.putFloat unsafe addr (unchecked-float prim-init-value))
       :float64 (.putDouble unsafe addr (unchecked-double prim-init-value)))
     nbuf))
  (^NativeBuffer [dtype prim-init-value]
   (make-ptr dtype prim-init-value {:resource-type :auto
                                    :uninitialized? true})))


(defprotocol PLibrarySingleton
  "Protocol to allow easy definition of a library singleton that manages re-creating
  the library definition when the library data changes and also recreating the
  library instance if necessary."
  (library-singleton-reset! [lib-singleton]
    "Regenerate library bindings and bind a new instance to allow repl-style
iterative development.")
  (library-singleton-set! [lib-singleton libpath]
    "Set the library path, generate the library and create a new instance.")
  (library-singleton-set-instance!
    [lib-singleton libinst]
    "In some cases such as graal native pathways you have to hard-set the definition and instance.")
  (library-singleton-find-fn [lib-singleton fn-name]
    "Find a bound function in the library.  Returns an implementation of
clojure.lang.IFn that takes only the specific arguments.")
  (library-singleton-find-symbol [lib-singleton sym-name]
    "Find a symbol in the library.  Returns a pointer.")
  ;;accessors
  (library-singleton-library-path [lib-singleton]
    "Get the bound library path")
  (library-singleton-definition [lib-singleton]
    "Return the library definition.")
  (library-singleton-library [lib-singleton]
    "Return the library instance"))


(deftype LibrarySingleton [library-def-var
                           library-symbols-var
                           library-def-options
                           ^:unsynchronized-mutable library-definition
                           ^{:unsynchronized-mutable true
                             :tag Library} library-instance
                           ^:unsynchronized-mutable library-path]
  IDeref
  (deref [this] library-instance)
  PLibrarySingleton
  (library-singleton-reset! [this]
    (graal-native/when-not-defined-graal-native
     (locking this
       (when library-path
         (set! library-definition (define-library @library-def-var
                                    @library-symbols-var
                                    library-def-options))
         (set! library-instance (instantiate-library library-definition
                                                     (:libpath library-path)))))))
  (library-singleton-set! [this libpath]
    (set! library-path {:libpath libpath})
    (library-singleton-reset! this))
  (library-singleton-set-instance! [lib-singleton libinst]
    (set! library-instance libinst))
  (library-singleton-find-fn [this fn-kwd]
    (errors/when-not-errorf
     library-instance
     "Library instance not found.  Has initialize! been called?")
    ;;derefencing a library instance returns a map of fn-kwd->fn
    (if-let [retval (fn-kwd @library-instance)]
      retval
      (errors/throwf "Library function %s not found" (symbol (name fn-kwd)))))
  (library-singleton-find-symbol [this sym-name]
    (errors/when-not-errorf
     library-instance
     "Library instance not found.  Has initialize! been called?")
    (.findSymbol library-instance sym-name))
  (library-singleton-library-path [lib-singleton] (:libpath library-path))
  (library-singleton-definition [lib-singleton] library-definition)
  (library-singleton-library [lib-singleton] library-instance))


(defn library-singleton
  "Create a singleton object, ideally assigned to a variable with defonce, that
  you can reset to auto-reload the bindings i.e. with new function definitions
  at the repl.

  * `library-def-var` must be something that deref's to the latest library definition.

```clojure
(defonce ^:private lib (dt-ffi/library-singleton #'avcodec-fns))

;;Safe to call on uninitialized library.  If the library is initialized, however,
;;a new library instance is created from the latest avcodec-fns
(dt-ffi/library-singelton-reset! lib)

(defn set-library!
  [libpath]
  (dt-ffi/library-singelton-set! lib libpath))
(defn- find-avcodec-fn
  [fn-kwd]
  (dt-ffi/library-singelton-find-fn lib fn-kwd))
```"
  ([library-def-var library-sym-var library-def-opts]
   (LibrarySingleton. library-def-var library-sym-var library-def-opts nil nil nil))
  ([library-def-var]
   (library-singleton library-def-var (delay nil) nil)))


(defmacro define-library-functions
  "Define public callable vars that will call library functions marshaling strings
  back and forth.  These vars will call find-fn with the fn-name in a late-bound way
  and check-error may be provided and called on a return value assuming :check-error?
  is set in the library definition.

  * library-def-symbol - The fully namespaced symbol that points to the function definitions.
  * find-fn - A function taking one argument - the fn name, and returning the function.
  * check-error - A function or macro that receives two arguments - the fn definition
    from above and the actual un-evaluated function call allowing you to insert pre/post
    checks.

Example:

```clojure
(dt-ffi/define-library-functions avclj.ffi/avcodec-fns find-avcodec-fn check-error)
```"
  [library-def-symbol find-fn check-error]
  `(do
     (let [~'find-fn ~find-fn]
       ~@(->>
          @(resolve library-def-symbol)
          (map
           (fn [[fn-name {:keys [rettype argtypes check-error?] :as fn-data}]]
             (let [fn-symbol (symbol (name fn-name))
                   requires-resctx? (first (filter #(= :string %)
                                                   (concat (map second argtypes)
                                                           [rettype])))
                   fn-def `(~'ifn ~@(map
                                     (fn [[argname argtype]]
                                       (cond
                                         (#{:int8 :int16 :int32 :int64} argtype)
                                         `(long ~argname)
                                         (#{:float32 :float64} argtype)
                                         `(double ~argname)
                                         (= :string argtype)
                                         `(string->c ~argname)
                                         :else
                                         argname))
                                     argtypes))]
               `(defn ~fn-symbol
                  ~(:doc fn-data "No documentation!")
                  ~(mapv first argtypes)
                  (let [~'ifn (~'find-fn ~fn-name)]
                    (do
                      ~(if requires-resctx?
                         `(resource/stack-resource-context
                           (let [~'retval ~(if (and check-error check-error?)
                                             `(~check-error ~fn-data ~fn-def)
                                             `~fn-def)]
                             ~(if (= :string rettype)
                                `(c->string ~'retval)
                                `~'retval)))
                         `(let [~'retval ~(if (and check-error check-error?)
                                             `(~check-error ~fn-data ~fn-def)
                                             `~fn-def)]
                            ~'retval))))))))))))


(defn ptr->struct
  "Given a struct type and a pointer return a struct whose data starts
  at the address of the pointer.

```clojure
  (let [codec (dt-ffi/ptr->struct (:datatype-name @av-context/codec-def*) codec-ptr)]
       ...)
```"
  ^Map [struct-type ptr]
  (let [n-bytes (:datatype-size (dt-struct/get-struct-def struct-type))
        src-ptr (->pointer ptr)
        nbuf (native-buffer/wrap-address (.address src-ptr)
                                         n-bytes
                                         src-ptr)]
    (dt-struct/inplace-new-struct struct-type nbuf)))


(defn struct-member-ptr
  "Get a pointer to a struct data member.

```clojure
   (swscale/sws_scale sws-ctx
                      (struct-member-ptr input-frame :data)
                      (struct-member-ptr input-frame :linesize)
                      0 (:height input-frame)
                      (struct-member-ptr encoder-frame :data)
                      (struct-member-ptr encoder-frame :linesize))
```"
  ^Pointer [data-struct member]
  (let [data-ptr (->pointer data-struct)
        member-offset (-> (dt-struct/offset-of
                           (dt-struct/get-struct-def (dtype-proto/datatype
                                                      data-struct))
                           member)
                          ;;offset-of returns a pair of offset,datatype
                          (first))]
    (Pointer. (+ (.address data-ptr) member-offset))))
