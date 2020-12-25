(ns tech.v3.datatype.mmap-string-list
  (:require [clojure.java.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.mmap :as mmap])
  (:import [tech.v3.datatype ObjectBuffer PrimitiveList]))

;; (defn append-to-file [fpath bytes]
;;   (with-open [o (io/output-stream fpath :append true)]
;;     (.write o bytes)))
(set! *warn-on-reflection* true)

(defn extract-string [mmap offset length]
  (String.
   (dtype/->byte-array
    (dtype/sub-buffer mmap offset length))))


(deftype MmapStringList [fpath ^java.io.OutputStream output-stream positions is-mmap-dirty? mmap]

  ObjectBuffer
  (elemwiseDatatype [this] :string)
  (lsize [this] (count  @positions ))
  (allowsRead [this] true)
  (allowsWrite [this] false)
  (readObject [this idx]
    (if @is-mmap-dirty?
       (do (reset! mmap (mmap/mmap-file fpath))
           (reset! is-mmap-dirty? false)))

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
        (reset! is-mmap-dirty? true)
        )))
  (addBoolean [this value] (.addObject value))
  (addDouble [this value] (.addObject value))
  (addLong [this value] (.addObject value)))

(comment
  (spit "/tmp/test.mmap" "")

  (def my-list
    (->MmapStringList "/tmp/test.mmap"
                      (io/output-stream "/tmp/test.mmap" :append true)
                      (atom [])
                      (atom true)
                      (atom (mmap/mmap-file "/tmp/test.mmap"))))

  (time
   (def _
     (doall
      (repeatedly  1000000
                   #(.addObject my-list "hello world")))))

  (time
   (run!
    #(.readObject ^PrimitiveList my-list %)
    (range 1000000)))


  )
