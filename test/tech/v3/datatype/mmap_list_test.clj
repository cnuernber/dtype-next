(ns tech.v3.datatype.mmap-list-test
  (:require [tech.v3.datatype.mmap :as sut]
            [clojure.test :as t]
            [tech.v3.datatype.mmap :as mmap]
            [clojure.java.io :as io]
            [tech.v3.datatype :as dtype]
            )

(:import
           [tech.v3.datatype PrimitiveList Buffer]
           )
  )

(defn append-to-file [fpath bytes]
  (with-open [o (io/output-stream fpath :append true)]
    (.write o bytes)))

(defn extract-string [mmap offset length]
     (String.
      (dtype/->byte-array
       (dtype/sub-buffer mmap offset length))))


(deftype MmapPrimitiveList [fpath positions]

  Buffer
  (elemwiseDatatype [this])
  (lsize [this] (count  @positions ))
  (allowsRead [this] true)
  (allowsWrite [this] true)
  (readBoolean [this idx] )
  (readByte [this idx] )
  (readShort [this idx] )
  (readChar [this idx] )
  (readInt [this idx] )
  (readLong [this idx] )
  (readFloat [this idx] )
  (readDouble [this idx])
  (readObject [this idx]
    (let [mmap (mmap/mmap-file fpath)
          current-positions (nth @positions idx)
          ]
      (extract-string
       mmap
       (first current-positions)
       (second current-positions)
       ))
    )
  (writeBoolean [this idx val] )
  (writeByte [this idx val] )
  (writeShort [this idx val])
  (writeChar [this idx val] )
  (writeInt [this idx val] )
  (writeLong [this idx val] )
  (writeFloat [this idx val] )
  (writeDouble [this idx val] )
  (writeObject [this idx val] )


  PrimitiveList
  ;; (elemwiseDatatype [this] :int32)

  ;; (lsize [this] 0)

  (ensureCapacity [item new-size])

  (addBoolean [this value])

  (addDouble [this value])

  (addObject [this value]
    (let [bytes (.getBytes value)
          file-length (.length (io/file fpath))
          ]
      (swap! positions #(conj % [file-length (count bytes)]))
      (append-to-file fpath bytes ))

    )

  (addLong [this value]))

(comment

  (def positions (atom []))
  (def my-list (MmapPrimitiveList. "/tmp/test.mmap" positions))
  (.addObject my-list "hello")
  (.lsize my-list)
  (.readObject my-list 0)
  (.addObject my-list "super world")
  (.lsize my-list)
  (.readObject my-list 0)

  @positions

  (.lsize my-list)
  (.nth my-list 1)


  )
