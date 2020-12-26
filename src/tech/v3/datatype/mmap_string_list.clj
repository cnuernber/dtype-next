(ns tech.v3.datatype.mmap-string-list
  (:require [clojure.java.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.mmap :as mmap])
  (:import [tech.v3.datatype ObjectBuffer PrimitiveList]))

 (defn append-to-file [fpath bytes]
   (with-open [o (io/output-stream fpath :append true)]
     (.write o bytes)))

(set! *warn-on-reflection* true)



(deftype MmapStringList [fpath ^java.io.OutputStream output-stream positions mmap]

  ObjectBuffer
  (elemwiseDatatype [this] :string)
  (lsize [this] (count  @positions ))
  (allowsRead [this] true)
  (allowsWrite [this] false)
  (readObject [this idx]
    (if (nil? mmap)
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

  (defn extract-string [mmap offset length]
    (String.
     (dtype/->byte-array
      (dtype/sub-buffer mmap offset length))))


  (deftype MmapStringList-1 [fpath positions mmap]

    ObjectBuffer
    (elemwiseDatatype [this] :string)
    (lsize [this] (count  @positions ))
    (allowsRead [this] true)
    (allowsWrite [this] false)
    (readObject [this idx]
      (if (nil? mmap)
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
