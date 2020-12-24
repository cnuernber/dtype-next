(ns tech.v3.datatype.mmap-string-list
  (:require [clojure.java.io :as io]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.mmap :as mmap])
  (:import [tech.v3.datatype ObjectBuffer PrimitiveList]))

(defn append-to-file [fpath bytes]
  (with-open [o (io/output-stream fpath :append true)]
    (.write o bytes)))

(defn extract-string [mmap offset length]
     (String.
      (dtype/->byte-array
       (dtype/sub-buffer mmap offset length))))


(deftype MmapStringList [fpath positions]

  ObjectBuffer
  (elemwiseDatatype [this] :string)
  (lsize [this] (count  @positions ))
  (allowsRead [this] true)
  (allowsWrite [this] false)
  (readObject [this idx]
    (let [mmap (mmap/mmap-file fpath)
          current-positions (nth @positions idx)]
      (extract-string
       mmap
       (first current-positions)
       (second current-positions))))
  (writeObject [this idx val] (throw (RuntimeException. "Writing not supported")))

  PrimitiveList
  ;; (elemwiseDatatype [this] :int32)

  ;; (lsize [this] 0)

  (ensureCapacity [item new-size])


  (addObject [this value]
    (if (not  (instance? CharSequence value ))
      (throw (RuntimeException. "Only :string is upported"))
      (let [bytes (.getBytes value)
            file-length (.length (io/file fpath))]
        (swap! positions #(conj % [file-length (count bytes)]))
        (append-to-file fpath bytes ))))
  (addBoolean [this value] (.addObject value))
  (addDouble [this value] (.addObject value))
  (addLong [this value] (.addObject value)))
