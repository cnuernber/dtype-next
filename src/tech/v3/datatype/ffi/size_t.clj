(ns tech.v3.datatype.ffi.size-t)


(def ^{:dynamic true
       :tag 'long}
  size-t-size (if (#{"x86_64" "amd64" "arm64" "aarch64"} (.toLowerCase (System/getProperty "os.arch")))
                    8
                    4))

(def ^{:dynamic true
       :tag 'long}
  ptr-t-size size-t-size)

(defmacro with-size-t-size
  [new-size & body]
  `(with-bindings {#'size-t-var new-size}
     ~@body))


(defn size-t-type
  "the size-t datatype - either `:uint32` or `:uint64`."
  []
  (if (== size-t-size 8)
    :uint64
    :uint32))


(defn offset-t-type
  "the offset-t datatype - either `:int32` or `:int64`."
  []
  (if (== size-t-size 8)
    :int64
    :int32))


(defn ptr-t-type
  "the ptr-t-type datatype - either `:uint32` or `:uint64`."
  []
  (if (= size-t-size 8)
    :uint64
    :uint32))


(defn ^:no-doc lower-type
  "Downcast `:size-t` to its signed integer equivalent."
  [argtype]
  (case argtype
    :size-t (offset-t-type)
    :offset-t (offset-t-type)
    :string :pointer
    argtype))


(defn ^:no-doc numeric-size-t-type
  "Downcast size-t and pointers to their signed integer equivalents."
  [argtype]
  (if (#{:size-t :string :pointer :pointer? :offset-t} argtype)
    (offset-t-type)
    argtype))
