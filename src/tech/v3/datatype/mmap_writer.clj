3.(ns tech.v3.datatype.mmap-writer
  "Provides the ability to efficiently write binary data to a file with an
  automatic conversion to a native-buffer using mmap.

  Endianness can be provided and strings are automatically saved as UTF-8."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.casting :as casting]
            ;;support for nio buffer conversions
            [tech.v3.datatype.nio-buffer]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.resource :as resource]
            [clojure.tools.logging :as log])
  (:import [java.nio ByteBuffer ByteOrder]
           [java.nio.channels FileChannel]
           [java.nio.file Paths StandardOpenOption OpenOption]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype ObjectBuffer DataWriter]))


(set! *warn-on-reflection* true)


(defn- convert-to-bytes
  ^ArrayBuffer [^ArrayBuffer data endianness]
  (let [buf-dtype (dtype/elemwise-datatype data)
        byte-width (casting/numeric-byte-width buf-dtype)
        host-dtype (casting/host-flatten buf-dtype)
        n-elems (dtype/ecount data)
        byte-buf (ByteBuffer/wrap (byte-array (* byte-width n-elems)))
        _ (if (= :little-endian endianness)
            (.order byte-buf ByteOrder/LITTLE_ENDIAN)
            (.order byte-buf ByteOrder/BIG_ENDIAN))
        offset (int (.offset data))]
    (case host-dtype
      :int8 (.put byte-buf ^bytes (.ary-data data)
                  offset
                  (int n-elems))
      :int16 (.put (.asShortBuffer byte-buf)
                   ^shorts (.ary-data data)
                   offset
                  (int n-elems))
      :int32 (.put (.asIntBuffer byte-buf)
                   ^ints (.ary-data data)
                   offset
                   (int n-elems))
      :int64 (.put (.asLongBuffer byte-buf)
                   ^longs (.ary-data data)
                   offset
                   (int n-elems))
      :float32 (.put (.asFloatBuffer byte-buf)
                     ^floats (.ary-data data)
                     offset
                     (int n-elems))
      :float64 (.put (.asDoubleBuffer byte-buf)
                     ^doubles (.ary-data data)
                     offset
                     (int n-elems)))
    (dtype-proto/->array-buffer byte-buf)))


(deftype MMapWriter [fpath
                     ^FileChannel file-channel
                     endianness
                     mmap-file-options
                     close-ref*]
  DataWriter
  (elemwiseDatatype [this] :int8)
  (lsize [this] (.position file-channel))
  (writeBytes [this byte-data off len]
    (.write file-channel (ByteBuffer/wrap ^bytes byte-data (int off) (int len))))
  (writeData [this data]
    (when-not (== 0 (dtype/ecount data))
      (if (string? data)
        (.writeBytes this (.getBytes ^String data))
        (let [data-dtype (dtype/elemwise-datatype data)
              data-shape (dtype/shape data)]
          (errors/when-not-errorf (casting/numeric-type? data-dtype)
            "Datatype %s is not numeric" data-dtype)
          (errors/when-not-errorf (== 1 (count data-shape))
            "Data should is not rank 1 (%d)" (count data-shape))
          (let [data-ary (dtype-cmc/->array-buffer data)
                ^ArrayBuffer data-ary (if (== 1
                                              (casting/numeric-byte-width data-dtype))
                                        data-ary
                                        (convert-to-bytes data-ary endianness))]
            (.writeBytes this (.ary-data data-ary)
                         (.offset data-ary) (.n-elems data-ary)))))))

  java.lang.AutoCloseable
  (close [this] @close-ref*)

  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [this] true)
  (->native-buffer [this]
    (.force file-channel true)
    (mmap/mmap-file fpath (merge {:endianness endianness}
                                 mmap-file-options))))

(def ^:private shutdown-hook
  (delay
   (let [shutdown-list (java.util.ArrayList.)]
     ;;First add the hook to ensure all files are deleted.
     (.addShutdownHook
      (Runtime/getRuntime)
      (Thread. ^Runnable #(locking shutdown-list
                            (doseq [close-ref* shutdown-list]
                              (try
                                @close-ref*
                                (catch Throwable e
                                  (log/error e "mmap-writer shutdown failure")))))
               "tech.v3.datatype.mmap-writer shutdown hook"))
     ;;Second, return a function to add more shutdown refs to the list
     (fn [close-ref*]
       (locking shutdown-list
         (.add shutdown-list close-ref*))))))


(defn mmap-writer
  "Create a new data writer that has a conversion to a mmaped native buffer upon
  ->buffer or ->reader.  Object metadata is passed to the mmap-file method so users
  can specify how the new file should interact with the resource system.

  Binary data will be encoded to bytes using the endianness provided in the options.
  Strings are encoded to UTF-8 but *may not be zero terminated!!*.

  Options:

  * `:file-channel` - Provide your own file channel.  Else one is created via fpath
     and open-options.
  * `:endianness` - defaults to platform endianness, either `:little-endian` or
    `:big-endian`.
  * `:open-options` - defaults to `:overwrite` - either `:overwrite` or `:append`.
  * `:resource-type` - defaults to :auto, one of the tech.v3.resource/track
     track-type  options.
  * `:mmap-file-options` - options to pass to mmap-file.
  * `:delete-on-close?` - defaults to false - When true, the file is deleted upon
     close."
  (^DataWriter [fpath {:keys [endianness resource-type mmap-file-options
                              open-options file-channel delete-on-close?]
                       :or {resource-type :auto
                            endianness (dtype-proto/platform-endianness)
                            open-options :append}
                       :as options}]
   (let [fpath (str fpath)
         file-channel
         (or file-channel
             (FileChannel/open
              (Paths/get (str fpath)
                         ^"L[java.lang.String;" (make-array String 0))
              (into-array (concat (case open-options
                                    :append [StandardOpenOption/APPEND
                                             StandardOpenOption/CREATE
                                             StandardOpenOption/WRITE]
                                    :overwrite [StandardOpenOption/CREATE
                                                StandardOpenOption/TRUNCATE_EXISTING
                                                StandardOpenOption/WRITE])))))
         _ (errors/when-not-errorf (instance? FileChannel file-channel)
             "Argument 'file-channel' (%s) is not an instance of a java.nio.FileChannel."
             file-channel)
         close-ref* (delay (do
                             (log/debugf "closing %s" fpath)
                             (.close ^FileChannel file-channel)
                               (when delete-on-close?
                                 (.delete (java.io.File. fpath)))))
         retval
         (MMapWriter. fpath
                      file-channel
                      endianness
                      (merge {:resource-type :auto} mmap-file-options)
                      close-ref*)]
     ;;Guarantee this file will be deleted when the JVM shuts down.
     (when (and delete-on-close?
                (= :gc (resource/normalize-track-type resource-type)))
       (@shutdown-hook close-ref*))
     (resource/track retval {:track-type resource-type
                             :dispose-fn #(deref close-ref*)})))
  (^DataWriter [fpath] (mmap-writer fpath nil)))


(defn temp-mmap-writer
  "Create a temporary mmap writer.

  Options -- See options for mmap-writer.  In addition:

  * `:delete-on-close` - defaults to true.
  * `:temp-dir` - Temporary directory.  Defaults to the system property
     \"java.io.tmpdir\".
  * `:suffix` - Suffix to use to create the file.  Defaults to nothing."
  (^DataWriter [{:keys [temp-dir suffix delete-on-close?]
                 :or {temp-dir (System/getProperty "java.io.tmpdir")
                      suffix ""
                      delete-on-close? true}
                 :as options}]
   (let [fname (str "dtype-next-mmap-writer-"(java.util.UUID/randomUUID) suffix)
         fullpath (str (Paths/get (str temp-dir) (into-array String [fname])))]
     (mmap-writer fullpath (assoc options :delete-on-close? delete-on-close?))))
  (^DataWriter [] (temp-mmap-writer nil)))


(defn path
  "Get the path used for a particular mmap-writer"
  [^MMapWriter mmap-writer]
  (.fpath mmap-writer))
