(ns tech.v3.datatype.mmap-writer
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            ;;support for nio buffer conversions
            [tech.v3.datatype.nio-buffer]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.resource :as resource])
  (:import [java.net URI]
           [java.nio ByteBuffer ByteOrder]
           [java.nio.channels FileChannel]
           [java.nio.file Paths StandardOpenOption]
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
                     mmap-file-options]
  DataWriter
  (elemwiseDatatype [this] :int8)
  (lsize [this] (.position file-channel))
  (writeBytes [this byte-data off len]
    (.write file-channel (ByteBuffer/wrap ^bytes byte-data (int off) (int len))))
  (writeData [this data]
    (when-not (== 0 (dtype/ecount data))
      (let [data-dtype (dtype/elemwise-datatype data)
            data-shape (dtype/shape data)]
        (errors/when-not-errorf (casting/numeric-type? data-dtype)
          "Datatype %s is not numeric" data-dtype)
        (errors/when-not-errorf (== 1 (count data-shape))
          "Data should is not rank 1 (%d)" (count data-shape))
        (let [data-ary (dtype-cmc/->array-buffer data)
              ^ArrayBuffer data-ary (if (== 1 (casting/numeric-byte-width data-dtype))
                                      data-ary
                                      (convert-to-bytes data-ary endianness))]
          (.writeBytes this (.ary-data data-ary)
                       (.offset data-ary) (.n-elems data-ary))))))

  dtype-proto/PToBuffer
  (convertible-to-buffer? [this] true)
  (->buffer [this]
    (.force file-channel true)
    (mmap/mmap-file fpath (merge {:endianness endianness}
                                 mmap-file-options)))

  dtype-proto/PToReader
  (convertible-to-reader? [this] true)
  (->reader [this] (dtype-proto/->buffer this))

  dtype-proto/PToWriter
  (convertible-to-writer? [this] false))


(defn mmap-writer
  "Create a new data writer that has a conversion to a mmaped native buffer upon
  ->buffer or ->reader.  Object metadata is passed to the mmap-file method so users
  can specify how the new file should interact with the resource system.

  Options:

  * `:file-channel` - Provide your own file channel.  Else one is created via fpath
     and open-options.
  * `:endianness` - defaults to platform endianness, either `:little-endian` or
    `:big-endian`.
  * `:open-options` - defaults to `:overwrite` - either `:overwrite` or `:append`.
  * `:resource-type` - defaults to :auto, one of the tech.v3.resource/track
     track-type  options.
  * `:mmap-file-options` - options to pass to mmap-file."
  (^DataWriter [fpath {:keys [endianness resource-type mmap-file-options
                              open-options file-channel]
                       :or {resource-type :auto
                            endianness (dtype-proto/platform-endianness)
                            open-options :overwrite}
                       :as options}]
   (let [file-channel (or file-channel
                          (FileChannel/open
                           (Paths/get (str fpath)
                                      ^"L[java.lang.String;" (make-array String 0))
                           (into-array (case open-options
                                         :append [StandardOpenOption/APPEND]
                                         :overwrite [StandardOpenOption/CREATE]))))
         _ (errors/when-not-errorf (instance? FileChannel file-channel)
             "Argument 'file-channel' (%s) is not an instance of a java.nio.FileChannel."
             file-channel)
         retval
         (MMapWriter. fpath
                      file-channel
                      endianness
                      mmap-file-options)]
     (resource/track retval {:track-type resource-type
                             :dispose-fn #(.close ^FileChannel file-channel)})))
  (^DataWriter [fpath] (mmap-writer fpath nil)))

(comment
  (require '[criterium.core :as crit])
  (defn write-100M-data [my-list]
    (doall
     (repeatedly 1000000
                 #(do
                    (.addObject my-list (apply str (repeat 100 "a"))))))
    nil)


  (defn write-1M-data [my-list]
    (doall
     (repeatedly 10000
                 #(do
                    (.addObject my-list (apply str (repeat 100 "a")))
                    )))
    nil)

  (do
    (crit/quick-bench
     (do
       (spit "/tmp/test.mmap" "")
       (write-1M-data

        (->MmapList
         #(String. %)
         #(.getBytes %)
         :string
         "/tmp/test.mmap"
         (FileChannel/open  (Paths/get  (URI. "file:/tmp/test.mmap"))
                            (into-array [StandardOpenOption/APPEND]))
         (atom [])
         (atom nil))))

     )
    ;; ->   Execution time mean : 140.223013 ms
    ;; ->   72 ms with FileOutputStream
    ;; ->   144 with FileChannel

    ;; (crit/quick-bench
    ;;  (do
    ;;    (spit "/tmp/test.mmap" "")
    ;;    (write-1M-data
    ;;     (->MmapStringList-1 "/tmp/test.mmap"
    ;;                         (atom [])
    ;;                         (atom nil)))))

    ;; ->  Execution time mean : 377.907873 ms
    ))

(comment
  (def my-list
    (->MmapList
     #(String. %)
     #(.getBytes %)
     :string
     "/tmp/test.mmap"
     (FileChannel/open  (Paths/get  (URI. "file:/tmp/test.mmap"))
                        (into-array [StandardOpenOption/APPEND]))
     (atom [])
     (atom nil)

     ))

  (.addObject my-list "this is a test")
  (first my-list)
  )
