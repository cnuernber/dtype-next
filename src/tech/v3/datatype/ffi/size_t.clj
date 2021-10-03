(ns tech.v3.datatype.ffi.size-t
  (:import [java.util.concurrent ConcurrentHashMap]
           [java.util.function Function]))


(def arch64-set #{"x86_64" "amd64" "arm64" "aarch64"})


(defn platform-size-t-size
  ^long []
  (let [arch (.toLowerCase (System/getProperty "os.arch"))]
    (if (arch64-set arch)
      8
      4)))


(def ^{:tag ConcurrentHashMap}
  size-t-size* (ConcurrentHashMap.))


(defn size-t-size
  "Get the size in bytes of a size-t integer."
  ^long []
  (long (.computeIfAbsent size-t-size* (.getId (Thread/currentThread))
                          (reify Function
                            (apply [this arg]
                              (platform-size-t-size))))))


(defmacro with-size-t-size
  [new-size & body]
  `(let [old-size# (size-t-size)
         tid# (.getId (Thread/currentThread))]
     (.put size-t-size* tid# (long ~new-size))
     (try
       ~@body
       (finally
         (.put size-t-size* tid# old-size#)))))


(defn size-t-type
  "the size-t datatype - either `:uint32` or `:uint64`."
  []
  (if (= (size-t-size) 8)
    :uint64
    :uint32))


(defn offset-t-type
  "the offset-t datatype - either `:int32` or `:int64`."
  []
  (if (= (size-t-size) 8)
    :int64
    :int32))


(defn ptr-t-type
  "the ptr-t-type datatype - either `:uint32` or `:uint64`."
  []
  (if (= (size-t-size) 8)
    :uint64
    :uint32))


(defn ^:no-doc lower-type
  "Downcast `:size-t` to its signed integer equivalent."
  [argtype]
  (case argtype
    :size-t (offset-t-type)
    :string :pointer
    argtype))


(defn ^:no-doc lower-ptr-type
  "Downcast size-t and pointers to their integer equivalents"
  [argtype]
  (if (#{:size-t :string :pointer :pointer?} argtype)
    (offset-t-type)
    argtype))
