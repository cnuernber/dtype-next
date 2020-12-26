(ns tech.v3.datatype.mmap-string-list
  (:require [clojure.java.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.mmap :as mmap])
  (:import [tech.v3.datatype ObjectBuffer PrimitiveList]))


(defn extract-string [mmap offset length]
  (String.
   (dtype/->byte-array
    (dtype/sub-buffer mmap offset length))))



(deftype MmapStringList [fpath ^java.io.OutputStream output-stream positions mmap]

  ObjectBuffer
  (elemwiseDatatype [this] :string)
  (lsize [this] (count  @positions ))
  (allowsRead [this] true)
  (allowsWrite [this] false)
  (readObject [this idx]
    (if (nil? @mmap)
      (reset! mmap (mmap/mmap-file fpath)))

    (let [ current-positions (nth @positions idx)]
      (extract-string
       @mmap
       (first current-positions)
       (second current-positions))))
  (writeObject [this idx val] (throw (RuntimeException. "Writing not supported")))

  PrimitiveList
  (ensureCapacity [item new-size])
  (addObject [this value]
    (if (not  (instance? CharSequence value ))
      (throw (RuntimeException. "Only :string is upported"))
      (let [^bytes bytes (.getBytes ^String value)
            file-length (.length (io/file fpath))]
        (swap! positions #(conj % [file-length (count bytes)]))
        (.write output-stream bytes)
        (.flush output-stream)
        (reset! mmap nil)
        )))
  (addBoolean [this value] (.addObject value))
  (addDouble [this value] (.addObject value))
  (addLong [this value] (.addObject value)))


(comment


  (defn append-to-file [fpath bytes]
    (with-open [o (io/output-stream fpath :append true)]
      (.write o bytes)))

  (deftype MmapStringList-1 [fpath positions mmap]

    ObjectBuffer
    (elemwiseDatatype [this] :string)
    (lsize [this] (count  @positions ))
    (allowsRead [this] true)
    (allowsWrite [this] false)
    (readObject [this idx]
      (if (nil? @mmap)
        (reset! mmap (mmap/mmap-file fpath)))

      (let [ current-positions (nth @positions idx)]
        (extract-string
         @mmap
         (first current-positions)
         (second current-positions))))
    (writeObject [this idx val] (throw (RuntimeException. "Writing not supported")))



    PrimitiveList
    (ensureCapacity [item new-size])
    (addObject [this value]
      (if (not  (instance? CharSequence value ))
        (throw (RuntimeException. "Only :string is upported"))
        (let [^bytes bytes (.getBytes ^String value)
              file-length (.length (io/file fpath))]
          (swap! positions #(conj % [file-length (count bytes)]))
          (append-to-file fpath bytes)
          (reset! mmap nil)
          )))
    (addBoolean [this value] (.addObject value))
    (addDouble [this value] (.addObject value))
    (addLong [this value] (.addObject value))))



(comment

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
        (->MmapStringList "/tmp/test.mmap"
                          (io/output-stream "/tmp/test.mmap" :append true)
                          (atom [])
                          (atom nil)))))
    ;; ->   Execution time mean : 140.223013 ms

    (crit/quick-bench
     (do
       (spit "/tmp/test.mmap" "")
       (write-1M-data
        (->MmapStringList-1 "/tmp/test.mmap"
                            (atom [])
                            (atom nil)))))

    ;; ->  Execution time mean : 377.907873 ms
    )

  )

