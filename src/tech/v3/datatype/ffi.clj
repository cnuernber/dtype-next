(ns tech.v3.datatype.ffi
  "Generalized C FFI interface that unifies JNA and JDK-16 FFI architectures."
  (:require [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.base :as base]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]
           [java.nio.charset Charset]))


(set! *warn-on-reflection* true)


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


(defrecord Pointer [^long address])


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
  String
  (convertible-to-pointer? [item] true)
  (->pointer [item]
    (let [str-bytes (.getBytes (str item))
          n-bytes (alength str-bytes)
          ;;nbuf is zero-initialized
          nbuf (dtype-cmc/make-container :native-heap :int8
                                         {:resource-type :auto}
                                         ;;pad with zero byte
                                         (inc n-bytes))]
      (dtype-cmc/copy! str-bytes (base/sub-buffer nbuf n-bytes))
      (->pointer nbuf)))
  NativeBuffer
  (convertible-to-pointer? [item] true)
  (->pointer [item]
    (Pointer. (.address item))))


(def ^:private ffi-impl (if (jdk-ffi?)
                          (requiring-resolve 'tech.v3.datatype.ffi-mmodel/ffi-fns)
                          (try
                            (requiring-resolve 'tech.v3.datatype.ffi-jna/ffi-fns)
                            (catch Throwable e
                              (throw (Exception. "Neither jdk-16 nor JNA are found on classpath.  FFI functionality is missing."))))))


(defn load-library
  "Load a library returning an implementation specific library instance.

  Exception if the library cannot be found."
  [libname]
  (if ((:library-instance? ffi-impl) libname)
    libname
    ((:load-library ffi-impl) libname)))


(defn find-symbol
  "Find a symbol in a library.  A library symbol is guaranteed to have a conversion to
  a pointer."
  ([libname symbol-name]
   ((:find-symbol ffi-impl) libname symbol-name))
  ([symbol-name]
   ((:find-symbol ffi-impl) nil symbol-name)))


(defn find-function
  "Find the symbol and return an IFn implementation wrapping typesafe access
  to the function.  The function's signature has to be completely specified.
  Supported datatypes are: `#{:void :int8 :uint8 :int16 :uint16 :int32 :uint32 :int64 :uint64 :float32 :float64 :size-t :pointer :pointer? :string}`.  This object's
  ->pointer method returns the actual function pointer.

  * `:void` is only used for return types.
  * `:pointer` is a pointer that may never be nil.
  * `:pointer?` is a pointer that may be nil.
  * `:string` - UTF-8 encoded string.
  * `:size-t` - either :int32 or :int64 depending on processor architecture.


  An automatic conversion of string to pointer is provided using UTF-8 encoding for
  the function call with the string being released after the function call.  Callers
  may also call 'string->c' and 'c->string'."
  [libname symbol-name rettype & [argtypes options]]
  ((:find-function ffi-impl) libname symbol-name rettype argtypes options))


(defn fn->c
  "Convert something implementing the Clojure IFn interface to a Pointer value.
  Callers must specify the C-interface they expect to be used.  See find-function
  for the definition of rettype and argtypes.  Returns a Pointer suitable to be
  passed to C functions as a callback with the given signature."
  ^Pointer [ifn rettype & [argtypes options]]
  ((:fn->pointer ffi-impl) ifn))
