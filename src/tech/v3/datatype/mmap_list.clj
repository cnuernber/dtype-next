(ns tech.v3.datatype.mmap-list
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.mmap :as mmap])
  (:import java.net.URI
           java.nio.ByteBuffer
           java.nio.channels.FileChannel
           [java.nio.file Paths StandardOpenOption]
           [tech.v3.datatype ObjectBuffer PrimitiveList]))

(set! *warn-on-reflection* true)

(defn extract-object [from-bytes-fn mmap offset length]
  (from-bytes-fn
   (dtype/->byte-array
    (dtype/sub-buffer mmap offset length))))



(deftype MmapList [from-bytes-fn
                   to-bytes-fn
                   elementwise-datatype
                   fpath
                   ^FileChannel file-channel
                   positions mmap]

  ObjectBuffer
  (elemwiseDatatype [this] elementwise-datatype)
  (lsize [this] (count @positions ))
  (allowsRead [this] true)
  (allowsWrite [this] false)
  (readObject [this idx]
    (if (nil? @mmap)
      (do
        (.force file-channel true)
        (reset! mmap (mmap/mmap-file fpath))))

    (let [ current-positions (nth @positions idx)]
      (extract-object
       from-bytes-fn
       @mmap
       (first current-positions)
       (second current-positions))))
  (writeObject [this idx val] (throw (RuntimeException. "Writing not supported")))

  PrimitiveList
  (ensureCapacity [item new-size])
  (addObject [this value]
    (let [^bytes bytes (to-bytes-fn value)
          file-length (.position file-channel)]
      (swap! positions #(conj % [file-length (count bytes)]))
      (.write file-channel (ByteBuffer/wrap bytes))
      (reset! mmap nil)))
  (addBoolean [this value] (.addObject value))
  (addDouble [this value] (.addObject value))
  (addLong [this value] (.addObject value)))



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
