(ns tech.v3.datatype.nio-buffer
  (:require [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.resource :as resource]
            [clojure.tools.logging :as log])
  (:import [java.nio Buffer ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer ByteOrder]
           [tech.v3.datatype UnsafeUtil]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype.array_buffer ArrayBuffer]))


(defn datatype->nio-buf-type
  [datatype]
  (case (casting/host-flatten datatype)
    :int8 java.nio.ByteBuffer
    :int16 java.nio.ShortBuffer
    :int32 java.nio.IntBuffer
    :int64 java.nio.LongBuffer
    :float32 java.nio.FloatBuffer
    :float64 java.nio.DoubleBuffer))


(defn as-byte-buffer ^ByteBuffer [item] item)
(defn as-short-buffer ^ShortBuffer [item] item)
(defn as-int-buffer ^IntBuffer [item] item)
(defn as-long-buffer ^LongBuffer [item] item)
(defn as-float-buffer ^FloatBuffer [item] item)
(defn as-double-buffer ^DoubleBuffer [item] item)



(defmacro datatype->nio-buf
  [datatype item]
  (case (casting/host-flatten datatype)
    :int8 `(as-byte-buffer ~item)
    :int16 `(as-short-buffer ~item)
    :int32 `(as-int-buffer ~item)
    :int64 `(as-long-buffer ~item)
    :float32 `(as-float-buffer ~item)
    :float64 `(as-double-buffer ~item)))


(def nio-datatypes #{:int8 :int16 :int32 :int64 :float32 :float64})


(defn buffer-address
  ^long [^Buffer buf]
  (.getLong (native-buffer/unsafe) ^Object buf UnsafeUtil/addressFieldOffset))


(defn buf->buffer
  [^Buffer buf]
  (let [cbuf (if (.isDirect buf)
               (dtype-proto/->native-buffer buf)
               (dtype-proto/->array-buffer buf))]
    (dtype-proto/->buffer cbuf)))


(defn extend-nio-types!
  []
  (doseq [dtype nio-datatypes]
    (let [buf-type (datatype->nio-buf-type dtype)]
      (extend buf-type
        dtype-proto/PElemwiseDatatype
        {:elemwise-datatype (constantly dtype)}
        dtype-proto/PElemwiseReaderCast
        {:elemwise-reader-cast buf->buffer}
        dtype-proto/PECount
        {:ecount #(.remaining ^Buffer %)}
        dtype-proto/PToArrayBuffer
        {:convertible-to-array-buffer? #(not (.isDirect ^Buffer %))
         :->array-buffer (fn [^Buffer buf]
                           (let [offset (.position buf)
                                 length (.remaining buf)]
                             (when-not (.isDirect buf)
                               (-> (array-buffer/array-buffer (.array buf))
                                   (dtype-proto/sub-buffer offset length)))))}
        dtype-proto/PToNativeBuffer
        {:convertible-to-native-buffer?  #(.isDirect ^Buffer %)
         :->native-buffer
         (fn [^Buffer buf]
           (native-buffer/wrap-address
            (buffer-address buf)
            (* (dtype-proto/ecount buf)
               (casting/numeric-byte-width (dtype-proto/elemwise-datatype buf)))
            (dtype-proto/elemwise-datatype buf)
            (dtype-proto/endianness buf)
            buf))}
        dtype-proto/PToBuffer
        {:convertible-to-buffer? (constantly true)
         :->buffer buf->buffer}
        dtype-proto/PToReader
        {:convertible-to-reader? (constantly true)
         :->reader buf->buffer}
        dtype-proto/PToWriter
        {:convertible-to-writer? (constantly true)
         :->writer buf->buffer}))))


(extend-nio-types!)


(def buffer-constructor*
  (delay (if UnsafeUtil/directBufferConstructor
           (fn [nbuf ^long address ^long nbytes _options]
             (let [retval
                   (UnsafeUtil/constructByteBufferFromAddress
                    address nbytes)]
               (resource/chain-resources retval nbuf)))
           (do
             (log/info "Unable to find direct buffer constructor -
falling back to jdk16 memory model.")
             (try
               (requiring-resolve 'tech.v3.datatype.ffi.nio-buf-mmodel/direct-buffer-constructor)
               (catch Exception e
                 (throw (RuntimeException. "Unable to load direct buffer constructor.  If you are using JDK-17, set your runtime :jvm-opts as follows:
:jvm-opts [\"--add-modules\" \"jdk.incubator.foreign,jdk.incubator.vector\"
                         \"--enable-native-access=ALL-UNNAMED\"]}"
                                          e))))))))


(defn native-buf->nio-buf
  (^java.nio.Buffer [^NativeBuffer buffer options]
   (let [dtype (dtype-proto/elemwise-datatype buffer)
         byte-width (casting/numeric-byte-width dtype)
         n-bytes (* (.n-elems buffer) byte-width)
         ^ByteBuffer byte-buf (@buffer-constructor* buffer (.address buffer) n-bytes
                               options)]
     (.order byte-buf
             (case (.endianness buffer)
               :little-endian ByteOrder/LITTLE_ENDIAN
               :big-endian ByteOrder/BIG_ENDIAN))
     (let [retval (case (casting/host-flatten dtype)
                    :int8 byte-buf
                    :int16 (.asShortBuffer byte-buf)
                    :int32 (.asIntBuffer byte-buf)
                    :int64 (.asLongBuffer byte-buf)
                    :float32 (.asFloatBuffer byte-buf)
                    :float64 (.asDoubleBuffer byte-buf))]
       (if (:resource-type options :auto)
         (resource/chain-resources retval buffer)
         retval))))
  (^java.nio.Buffer [buffer]
   (native-buf->nio-buf buffer nil)))


(defn as-nio-buffer
  "Convert to a nio buffer returning nil if not possible."
  (^Buffer [item options]
   (when-let [cbuf (dtype-base/as-concrete-buffer item)]
     (when (nio-datatypes (casting/host-flatten (dtype-base/elemwise-datatype cbuf)))
       (if (instance? NativeBuffer cbuf)
         (native-buf->nio-buf cbuf options)
         (let [^ArrayBuffer ary-buf cbuf
               pos (.offset ary-buf)
               limit (+ pos (.n-elems ary-buf))]
           (case (casting/host-flatten (dtype-base/elemwise-datatype cbuf))
             :int8 (doto (ByteBuffer/wrap (.ary-data ary-buf))
                     (.position pos)
                     (.limit limit))
             :int16 (doto (ShortBuffer/wrap (.ary-data ary-buf))
                      (.position pos)
                      (.limit limit))
             :int32 (doto (IntBuffer/wrap (.ary-data ary-buf))
                      (.position pos)
                      (.limit limit))
             :int64 (doto (LongBuffer/wrap (.ary-data ary-buf))
                      (.position pos)
                      (.limit limit))
             :float32 (doto (FloatBuffer/wrap (.ary-data ary-buf))
                        (.position pos)
                        (.limit limit))
             :float64 (doto (DoubleBuffer/wrap (.ary-data ary-buf))
                        (.position pos)
                        (.limit limit))))))))
  (^Buffer [item] (as-nio-buffer item nil)))


(defn ->nio-buffer
  "Convert to nio buffer throwing exception if not possible."
  (^Buffer [item options]
   (if-let [retval (as-nio-buffer item options)]
     retval
     (errors/throwf "Failed to convert item to nio buffer: %s" item)))
  (^Buffer [item]
   (->nio-buffer item nil)))
