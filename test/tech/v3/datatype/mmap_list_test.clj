(ns tech.v3.datatype.mmap-string-list-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.mmap-string-list :as string-list])
  (:import [tech.v3.datatype ObjectBuffer PrimitiveList]))

(deftest test-add-read
  (let [mmap-file (.getPath (java.io.File/createTempFile "strings" ".mmap") )
        positions (atom [])
        string-list (string-list/->MmapStringList mmap-file positions)]


    (.addObject string-list "test")
    (is (= 1 (.lsize string-list)))
    (is (= "test" (.readObject string-list 0)))))






(comment
  (deftype MmapPrimitiveList-2 [fpath positions output-stream]

    ObjectBuffer
    (elemwiseDatatype [this])
    (lsize [this] (count  @positions ))
    (allowsRead [this] true)
    (allowsWrite [this] true)
    (readObject [this idx]
      (let [mmap (mmap/mmap-file fpath)
            current-positions (nth @positions idx) ]
        (string-list/extract-string
         mmap
         (first current-positions)
         (second current-positions))))

    PrimitiveList
    (ensureCapacity [item new-size])
    (addBoolean [this value])
    (addDouble [this value])
    (addObject [this value]
      (let [bytes (.getBytes value)
            file-length (.length (io/file fpath))
            ]
        (swap! positions #(conj % [file-length (count bytes)]))
        (.write output-stream bytes)
        (.flush output-stream)))

    (addLong [this value]))

  (comment
    ;; (def positions (atom []))
    (use 'criterium.core)

    (defn write-1m-data [my-list]
      (doall
       (repeatedly 1000
                   #(do
                      (.addObject my-list (apply str (repeat 1000 "a")))
                      (.lsize my-list)
                      (.readObject my-list 0)
                      )))
      (.readObject my-list 999)
      nil)

    (quick-bench
     (do
       (spit "/tmp/test.mmap" "")
       (write-1m-data
        (string-list/->MmapStringList "/tmp/test.mmap" (atom [])))))

    ;; Execution time mean : 123.334262 ms

    (quick-bench
     (do
       (spit "/tmp/test.mmap" "")
       (write-1m-data
        (MmapPrimitiveList-2.
         "/tmp/test.mmap"
         (atom [])
         (io/output-stream "/tmp/test.mmap" :append true)))))

    ;;  Execution time mean : 105.334262 ms
    )


  (comment

    (def my-list (MmapPrimitiveList-1.  "/tmp/test.mmap" positions))
    (.addObject my-list "super world")
    (.lsize my-list)
    (.readObject my-list 0)
    (.writeObject my-list 0 "")

    @positions

    (.lsize my-list)
    (.nth my-list 1))

  )
