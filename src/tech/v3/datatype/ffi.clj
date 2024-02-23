(ns tech.v3.datatype.ffi
  "Generalized C Foreign Function Interface (FFI) that unifies JNA and JDK-16 FFI architectures.

  Users can dynamically define and load libraries and define callbacks that C can then
  call.

  This namespace is meant to work with the `struct` namespace where you can define
  C-structs laid out precisely in memory.

  Available datatypes for the binding layer:

  * `:int8` `:int16` `:int32` `:int64` `:float32` `:float64` `:pointer` `:pointer?`
    `:size-t`.  A `:pointer?` type means the pointer could potentially be null or zero while
    `:pointer` will throw an error if a null or zero pointer is passed in.
  * `:pointer`, `:pointer?`, `:size-t` are all types that change their underlying
     definition depending on if the system is 32 bit or 64 bit.



  Note that unsigned types will work but the interface will have to be defined in
  terms of their signed analogues.


  Arguments and return values can be specified as pass-by-value - **currently JNA only**.  This is the difference between
  the c signatures:
```c
  //pass-by-reference
  tdata* doit(adata* val);

  //pass-by-value
  tdata doti(adata val);
```

  In the return-by-value case the return value will be copied into a dtype struct of the correct datatype.
  In order to use pass or return by value you need to first (before interface instantiation) define the datatype:

```clojure
(defonce data-chunk-def*
  (delay
    (dt-struct/define-datatype! :duckdb-data-chunk
      [{:name :__dtck
        :datatype @ptr-dtype*}])))

(defonce appender-def*
  (delay
    (dt-struct/define-datatype! :duckdb-appender
      [{:name :__appn
        :datatype @ptr-dtype*}])))
  @data-chunk-def*
  @appender-def*


  ;;Then when defining your functions in your library use `(by-value struct-data-type)` in either
  ;;argtype or in rettype pathways.

(dt-ffi/define-library!
  'lib
  {
   ...
   ;;DUCKDB_API duckdb_state duckdb_append_data_chunk(duckdb_appender appender, duckdb_data_chunk chunk);
   :duckdb_append_data_chunk {:rettype :int32
                               :argtypes [[appender (by-value :duckdb-appender)]
                                          [data-chunk (by-value :duckdb-data-chunk)]]}

  ;;DUCKDB_API duckdb_data_chunk duckdb_create_data_chunk(duckdb_logical_type *types, idx_t column_count);
  :duckdb_create_data_chunk {:rettype (by-value :duckdb-data-chunk)
                             :argtypes [[types :pointer]
                                        [column-count :int64]]}
  })
```

  Under the covers during pass-by-value all the system needs is any valid java map of keyword property name
  to value and it will copy it into the appropriate structure just before call time.

  During return-by-value the data will be copied into a native-backed datatype struct with the :resource-type
  set to :auto.

  Datatype structs have accelerated .get and .reduce implementations so `(into {} data)` is a reasonable
  pathway if you want to convert from a struct to a persistent maps.

Example:

```clojure
user> (require '[tech.v3.datatype.ffi :as dtype-ffi])
nil
user> (dtype-ffi/define-library!
        clib
        '{:memset {:rettype :pointer
                   :argtypes [[buffer :pointer]
                              [byte-value :int32]
                              [n-bytes :size-t]]}
          :memcpy {:rettype :pointer
                   ;;dst src size-t
                   :argtypes [[dst :pointer]
                              [src :pointer]
                              [n-bytes :size-t]]}
          :qsort {:rettype :void
                  :argtypes [[data :pointer]
                             [nitems :size-t]
                             [item-size :size-t]
                             [comparator :pointer]]}}
        nil ;;no library symbols defined
        nil ;;no systematic error checking
        )

nil
user> ;;now we bind to a path or existing library.  nil means find
user> ;;symbols in current executable.
user> (dtype-ffi/library-singleton-set! clib nil)
#<G__17822@7f71640c:
  {:qsort #object[tech.v3.datatype.ffi.jna.G__17822$invoker_qsort 0x32b4a8cd \"tech.v3.datatype.ffi.jna.G__17822$invoker_qsort@32b4a8cd\"], :memset #object[tech.v3.datatype.ffi.jna.G__17822$invoker_memset 0x676ccb88 \"tech.v3.datatype.ffi.jna.G__17822$invoker_memset@676ccb88\"], :memcpy #object[tech.v3.datatype.ffi.jna.G__17822$invoker_memcpy 0x59fd3d8f \"tech.v3.datatype.ffi.jna.G__17822$invoker_memcpy@59fd3d8f\"]}>
user> ;;We can now call functions in our library.
user> (require '[tech.v3.datatype :as dtype])
nil
user> (def container (dtype/make-container :native-heap :float64 (range 10)))
#'user/container
user> (apply + container)
45.0
user> (memset container 0 (* (dtype/ecount container) 8))
#object[tech.v3.datatype.ffi.Pointer 0x59fa4fe0 \"{:address 0x00007F1B4458C0E0 }\"]
user> (apply + container)
0.0
user> ;;C callbacks take a bit more effort
user> ;;First define the callback signature.
user> (def comp-iface (dtype-ffi/define-foreign-interface :int32 [:pointer :pointer]))
#'user/comp-iface
user> ;;Then instantiate an implementation.
user> (import [tech.v3.datatype.ffi Pointer])
user> (require '[tech.v3.datatype.native-buffer :as native-buffer])
nil
user> (def iface-inst (dtype-ffi/instantiate-foreign-interface
                       comp-iface
                       (fn [^Pointer lhs ^Pointer rhs]
                         (let [lhs (.getDouble (native-buffer/unsafe) (.address lhs))
                               rhs (.getDouble (native-buffer/unsafe) (.address rhs))]
                           (Double/compare lhs rhs)))))
#'user/iface-inst
user> iface-inst
#object[tech.v3.datatype.ffi.jna.ffi_G__17831 0x6e9ddc45 \"tech.v3.datatype.ffi.jna.ffi_G__17831@6e9ddc45\"]
user> ;;From an instance of a foreign interface we can get an integer pointer
user> (def iface-ptr (dtype-ffi/foreign-interface-instance->c comp-iface iface-inst))
#'user/iface-ptr
user> iface-ptr
#object[tech.v3.datatype.ffi.Pointer 0x47099d5d \"{:address 0x00007F1BB400F390 }\"]
user> ;;reset container
user> (dtype/copy! (vec (shuffle (range 10))) container)
#native-buffer@0x00007F1B4458C0E0<float64>[10]
[5.000, 9.000, 8.000, 1.000, 3.000, 0.000, 6.000, 7.000, 4.000, 2.000]
user> (qsort container 10 8 iface-ptr)
nil
user> container
#native-buffer@0x00007F1B4458C0E0<float64>[10]
[0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000]
```

  Structs can be defined and passed by pointer/reference.  See the [[tech.v3.datatype.struct]]
  namespace.  Also note that the output of clang can be used to define your structs based on
  parsing c/c++ header files.  See [[tech.v3.datatype.ffi.clang/defstruct-from-layout]].


## Things To Consider:


  1.  If the structs are complex or involved I recommend using [clang to dump the record definitions](https://github.com/cnuernber/avclj/blob/master/cpptest/compile64.sh) and using those to [generate the struct layouts](https://github.com/cnuernber/avclj/blob/master/src/avclj/av_context.clj#L512) - [documentation](https://cnuernber.github.io/dtype-next/tech.v3.datatype.ffi.clang.html).)
  2.   Returning things by pointer-to-pointer.  This often happens in C interfaces so so there is a [pattern for this](https://github.com/cnuernber/tmducken/blob/main/src/tmducken/duckdb.clj#L181).
  3.  Sometimes you get a pointer to a struct and you need to access the struct values to get/set data on it.  You can cast any pointer to a struct [with this pattern](https://cnuernber.github.io/dtype-next/tech.v3.datatype.ffi.html#var-ptr-.3Estruct) and then it is an implementation of java.util.Map.)
  4.  If you want things to work on 32 bit systems then you will need to be careful with size-t, offset-t, and ptr definitions.  The [size-t namespace](https://cnuernber.github.io/dtype-next/tech.v3.datatype.ffi.size-t.html) is there to help out a bit.  I often use very late binding (delay) for systems that will have size-t definitions so that if someone were to AOT-compile the jar they don't happen to compile in the wrong size-t definition for a downstream system.
  5.  Transforming from a raw integer address into a native-buffer happens a lot - [wrap-address](https://cnuernber.github.io/dtype-next/tech.v3.datatype.native-buffer.html#var-wrap-address) with [example usage](https://github.com/cnuernber/avclj/blob/master/src/avclj.clj#L197).
  6.  It is possible with older creakier libraries you will need to pass in the address to struct members.  This [happened a lot in avclj](https://github.com/cnuernber/avclj/blob/master/src/avclj.clj#L266).


## GC


  What I often do is I get the pointer address, which is a primitive long value and thus not tracked by the GC system and reconstruct the pointer in the dispose-fn - [native-buffer's malloc is a good example of this](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/native_buffer.clj#L703).  This keeps the dispose fn from having a reference to the thing being tracked.   By convention I nearly always give users the ability to choose how they want the thing tracked with a :resource-type option that can be one of four possible options:

* nil - no tracking and data has to be manually released
* :stack - Throws an error if there isn't an open stack resource context but will clean things up.
* :gc - Always track via the GC tracking mechanism.
* :auto - nearly always the default, choose GC unless there is an open stack tracker.

  In the case of a derived object such as a child object of a parent allocated object I usually store the parent on the child's metadata so that the parent cannot get cleaned up before the child.  You can see this with the [sub-buffer call](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/native_buffer.clj#L399) in the case where someone malloc'd a large single buffer and then created child buffers still referencing the same data.


## A Word On Efficiency


Finally - on my favorite topic, efficiency, dtype-next has extremely fast copies from primitive arrays to native buffers of the same datatype and back.  Users can also write directly to native buffers as they can get a [buffer interface via ->buffer](https://github.com/cnuernber/dtype-next/blob/master/src/tech/v3/datatype/base.clj#L117).  That typehint means you are guaranteed to get an actual implementation of the [buffer interface](https://github.com/cnuernber/dtype-next/blob/master/java/tech/v3/datatype/Buffer.java#L20) so you can write single values somewhat efficiently but a bulk copy of a primitive array will be probably 100 times faster.

  My recommendation is to allow users who have large datasets to setup a steady state where they can write data to a double or float java primitive array which hotspot handles extremely well especially if everything is typehinted correctly and then use a single dtype/copy! call which will hit the memcpy fastpath for moving data into an actual native buffer.  Allocating primitive arrays is pretty quick but allocating large native arrays may not be and it will always be faster to setup a steady state where you are processing batches of data without allocating anything, preparing one batch while another batch is finishing.  You can see how a design like this makes the actual function call time nearly irrelevant because you are processing thousands of records with very few cross language function calls.  So the actual inter-language time of JNA vs. JDK-17 for a serious perf use case should be a nonissue as users need to handle intelligent batching that will involve nearly no C function calls or very few of them.

  A small detail talking about efficiency of writing data to a java primitive array - Clojure's aset returns the previous value and the Clojure compiler boxes it.   So Clojure's very own aset is a performance disadvantage compared to setting the value in Java or Scala or Kotlin.  For this reason in extremely tight loops I have a version of [aset that returns void](https://github.com/cnuernber/dtype-next/blob/master/java/tech/v3/datatype/ArrayHelpers.java#L7) and thus avoids boxing anything.  For arrays of 10 or 1000 things probably irrelevant but for tight loops running as fast as things can run those extra box calls are noticeable in the profiler."
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
  (boolean (or (try (Class/forName "java.lang.foreign.Linker") (catch Throwable e nil))
               (try (Class/forName "jdk.incubator.foreign.CLinker") (catch Throwable _e nil)))))


(defn jna-ffi?
  "Is JNA's ffi interface available?"
  []
  (try
    (boolean (Class/forName "com.sun.jna.Pointer"))
    (catch Throwable _e false)))


(defn jdk-mmodel?
  "Is the JDK native memory model available?"
  []
  (try
    (boolean (Class/forName "jdk.incubator.foreign.MemoryAddress"))
    (catch Throwable _e false)))


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
  ^String [data & {:keys [encoding]}]
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
    :jdk-21     (reset! ffi-impl* @(requiring-resolve 'tech.v3.datatype.ffi.mmodel-jdk21/ffi-fns))
    :jdk-19     (reset! ffi-impl* @(requiring-resolve 'tech.v3.datatype.ffi.mmodel-jdk19/ffi-fns))
    :jdk-pre-19 (reset! ffi-impl* @(requiring-resolve 'tech.v3.datatype.ffi.mmodel/ffi-fns))
    :jna        (reset! ffi-impl* @(requiring-resolve 'tech.v3.datatype.ffi.jna/ffi-fns))))

(defn- ffi-impl
  "Get an implementation of the actual FFI interface.  This is for internal use only."
  []
  ;;JNA must be first.  jdk-19 is too untested and doesn't work at this point in all contexts.
  (when (nil? @ffi-impl*)
    (try
      (set-ffi-impl! :jna)
      (catch Throwable e
        (log/debugf e "Failed to load JNA FFI implementation")
        (try
          (set-ffi-impl! :jdk-21)
          (catch Throwable e
            (log/debugf "Failed to load JDK 21 FFI implementation: %s" e)
            (try
              (set-ffi-impl! :jdk-19)
              (catch Throwable e
                (log/debugf "Failed to load JDK 19 FFI implementation: %s" e)
                (try
                  (set-ffi-impl! :jdk-pre-19)
                  (catch Throwable e
                    (log/error e "Failed to find a suitable FFI implementation.
Attempted :jdk, :jdk-pre-19, and :jna -- call set-ffi-impl! from the repl to see specific failure.)")
                    (reset! ffi-impl* :failed))))))))))
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
                     (catch Throwable _e false)))
       (first)))


(defn library-loadable?
  [libname]
  (boolean (find-library libname)))


(defn find-symbol
  "Find a symbol in a library.  A library symbol is guaranteed to have a conversion to
  a pointer."
  (^Pointer [libname symbol-name]
   ((:find-symbol (ffi-impl)) libname symbol-name))
  (^Pointer [symbol-name]
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
  (deref [_this] library-instance)
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
  (library-singleton-set-instance! [_lib-singleton libinst]
    (set! library-instance libinst))
  (library-singleton-find-fn [_this fn-kwd]
    (errors/when-not-errorf
     library-instance
     "Library instance not found.  Has initialize! been called?")
    ;;derefencing a library instance returns a map of fn-kwd->fn
    (if-let [retval (get @library-instance fn-kwd)]
      retval
      (errors/throwf "Library function %s not found" (symbol (name fn-kwd)))))
  (library-singleton-find-symbol [_this sym-name]
    (errors/when-not-errorf
     library-instance
     "Library instance not found.  Has initialize! been called?")
    (.findSymbol library-instance sym-name))
  (library-singleton-library-path [_lib-singleton] (:libpath library-path))
  (library-singleton-definition [_lib-singleton] library-definition)
  (library-singleton-library [_lib-singleton] library-instance))


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

(defn ^:private library-function-def* [[fn-name fn-data] check-error]
  (let [{:keys [rettype argtypes check-error?]} fn-data
        fn-symbol (symbol (name fn-name))
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
                 ~'retval)))))))


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
       ~@(->> (if (symbol? library-def-symbol)
                @(resolve library-def-symbol)
                library-def-symbol)
              (map #(library-function-def* % check-error))))))


(defmacro if-class
  ([class-name then]
   `(if-class ~class-name
      ~then
      nil))
  ([class-name then else?]
   (let [class-exists (try
                        (Class/forName (name class-name))
                        true
                        (catch ClassNotFoundException _e
                          false))]
     (if class-exists
       then
       else?))))

(defmacro define-library-interface
  "Define public callable vars that will call library functions marshaling strings
  back and forth. Returns a library singleton.

  * `fn-defs` - map of fn-name -> {:rettype :argtypes}
     * `argtypes` -
       * `:void` - return type only.
       * `:int8` `:int16` `:int32` `:int64`
       * `:float32` `:float64`
       * `:size-t` - int32 or int64 depending on cpu architecture.
       * `:pointer` `:pointer?` - Something convertible to a Pointer type.  Potentially
          exception when nil.
     * `rettype` - any argtype plus :void
     * `doc` - docstring for the function
     * `check-error?` apply pre/post checks with `check-error`. default: false

  Options:

  * `:classname` - If classname (a symbol) is provided a .class file is saved to
  *compile-path* after which `(import classname)` will be a validcall meaning that
  after AOT no further reflection or class generation is required to access the
  class explicitly.  That being said 'import' does not reload classes that are
  already on the classpath so this option is best used after library has stopped
  changing.

  * `:check-error` - A function or macro that receives two arguments - the fn definition
    from above and the actual un-evaluated function call allowing you to insert pre/post
    checks.

  * `:symbols` - A sequence of symbols in the shared library that should be available
    for use with `find-symbol`

  * `:libraries` - (graalvm only) A sequence of dependent shared libraries that should be loaded.

  * `:header-files` - (graalvm only) A sequence of header files.

  Example:

  ```clojure

  user> (dt-ffi/define-library-interface
          {:memset {:rettype :pointer
                    :argtypes [['p :pointer]
                               ['x :int32]
                               ['len :size-t]]}})
  user> (def test-buf (dtype/make-container :native-heap :float32 (range 10)))
  #'user/test-buf
  user> test-buf
  #native-buffer@0x00007F4E28CE56D0<float32>[10]
  [0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000, ]
  user> (memset test-buf 0 40)
  #object[tech.v3.datatype.ffi.Pointer 0x33013c57 \"{:address 0x00007F4E28CE56D0 }\"]
  user> test-buf
  #native-buffer@0x00007F4E28CE56D0<float32>[10]
  [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, ]
  ```
  "
  [fn-defs
   & {:keys [_classname
             check-error
             symbols
             libraries
             _header-files]
      :as opts}]
  (let [fn-defs-val (eval fn-defs)
        classname-form (:classname opts
                                   (list
                                    'quote
                                    ;; For graalvm, we want a consistent classname to make
                                    ;;    it easy to create reflection configurations.
                                    ;; For non graalvm, we want a unique classname
                                    ;;    to support repl redefinition.
                                    (graal-native/if-defined-graal-native
                                     (symbol (str (ns-name *ns* ) ".Bindings"))
                                     (symbol (str "tech.v3.datatype.ffi." (name (gensym)))))))

        library-options (-> (select-keys opts [:libraries :header-files])
                            (assoc :classname classname-form))
        library-options-val (eval library-options)
        classname (:classname library-options-val)]
    ;; Must compile class before returning macro response,
    ;; otherwise (new ~classname) will not succeed.
    ;; Also, cannot create instance dynamically
    ;; because of graalvm restrictions
    (graal-native/if-defined-graal-native
     (if *compile-files*
       ((requiring-resolve 'tech.v3.datatype.ffi.graalvm/define-library)
        fn-defs-val
        (eval symbols)
        library-options-val)
       (try
         (Class/forName ~(name classname))
         (catch ClassNotFoundException _e#
           nil)))
     (define-library
       fn-defs-val
       (eval symbols)
       library-options-val))

    `(let [fn-defs# (quote ~fn-defs-val)
           lib# (dt-ffi/library-singleton
                 (reify
                   IDeref
                   (deref [_#]
                     fn-defs#)))

           initialize# (delay
                         ;; load libraries
                         (doseq [library# ~libraries]
                           (find-library library#))

                         (dt-ffi/library-singleton-set! lib# nil)

                         (graal-native/when-defined-graal-native
                          (if-class ~classname
                            (do
                              (dt-ffi/library-singleton-set-instance! lib# (new ~classname)))
                            (do
                              (throw (Exception. "Library class does not exist"))))))

           find-fn# (fn [fn-kwd#]
                      @initialize#
                      (dt-ffi/library-singleton-find-fn lib# fn-kwd#))]

       (let [~'find-fn find-fn#]
         ~@(->> fn-defs-val
                (map #(library-function-def* % check-error)))
         lib#))))



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
                                         ptr)]
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


(defmacro define-library!
  "Define a library singleton that will export the defined functions and symbols.

  Only jvm-supported primitives work here - no unsigned but if you have the correct
  datatype widths the data will be passed unchanged so unsigned does work.

  See namespace declaration for full example.

  Example:

```clojure
  (define-library!
    lib
    '{:memset {:rettype :pointer
               :argtypes [[buffer :pointer]
                          [byte-value :int32]
                          [n-bytes :size-t]]}
      :memcpy {:rettype :pointer
               ;;dst src size-t
               :argtypes [[dst :pointer]
                          [src :pointer]
                          [n-bytes :size-t]]}
      :qsort {:rettype :void
              :argtypes [[data :pointer]
                         [nitems :size-t]
                         [item-size :size-t]
                         [comparator :pointer]]}}
    nil
    nil)
```
  "
  [lib-varname lib-fns lib-symbols error-checker]
  (let [lib-fns-var (symbol (str lib-varname "-fns"))
        lib-sym-var (symbol (str lib-varname "-symbols"))]
    `(do
       (def ~lib-fns-var ~lib-fns)
       (def ~lib-sym-var ~lib-symbols)
       (defonce ~(with-meta lib-varname {:tag LibrarySingleton}) (library-singleton (var ~lib-fns-var) (var ~lib-sym-var) nil))
       (define-library-functions ~lib-fns-var
         #(library-singleton-find-fn ~lib-varname %)
         ~error-checker)
       (library-singleton-reset! ~lib-varname))))

(comment

  (define-library!
    lib
    '{:memset {:rettype :pointer
               :argtypes [[buffer :pointer]
                          [byte-value :int32]
                          [n-bytes :size-t]]}
      :memcpy {:rettype :pointer
               ;;dst src size-t
               :argtypes [[dst :pointer]
                          [src :pointer]
                          [n-bytes :size-t]]}
      :qsort {:rettype :void
              :argtypes [[data :pointer]
                         [nitems :size-t]
                         [item-size :size-t]
                         [comparator :pointer]]}}
    nil
    nil)
  ;; We pass nil here because we are finding those symbols in the current executable.
  ;; We can also pass a path to a shared library.
  (library-singleton-set! lib nil)

  (require '[tech.v3.datatype :as dtype])
  (def container (dtype/make-container :native-heap :float64 (range 10)))
  (apply + container) ;;45.0
  (memset container 0 (* (dtype/ecount container) 8))
  (apply + container) ;; 0.0

  ;;C callbacks - qsort's comparator is passed a function
  ;;that takes 2 pointers and returns an int32
  (def comp-iface (define-foreign-interface :int32 [:pointer :pointer]))
  (def iface-inst (instantiate-foreign-interface
                   comp-iface
                   (fn [^Pointer lhs ^Pointer rhs]
                     (let [lhs (.getDouble (native-buffer/unsafe) (.address lhs))
                           rhs (.getDouble (native-buffer/unsafe) (.address rhs))]
                       (Double/compare lhs rhs)))))
  (def iface-ptr (foreign-interface-instance->c comp-iface iface-inst))
  (dtype/copy! (vec (shuffle (range 10))) container)
  (vec container) ;;[5.0 3.0 7.0 0.0 8.0 6.0 4.0 1.0 9.0 2.0]
  (qsort container 10 8 iface-ptr)
  (vec container) ;;[0.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0]
  )
