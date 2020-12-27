(ns tech.v3.datatype.mmap-string-list-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype.mmap-string-list :as string-list])
  (:import java.net.URI
           java.nio.channels.FileChannel
           [java.nio.file Paths StandardOpenOption]))

(deftest test-add-read
  (let [mmap-file (java.io.File/createTempFile "strings" ".mmap")
        positions (atom [])
        string-list (string-list/->MmapStringList
                     (.getPath mmap-file)
                     (FileChannel/open  (.toPath mmap-file)
                                        (into-array [StandardOpenOption/APPEND]))

                     positions
                     (atom nil)
                     )]


    (.addObject string-list "test")
    (is (= 1 (.lsize string-list)))
    (is (= "test" (.readObject string-list 0)))
    ))

(deftest test-add-read-varoius
  (let [mmap-file (java.io.File/createTempFile "strings" ".mmap")
        positions (atom [])
        string-list (string-list/->MmapStringList
                     (.getPath mmap-file)
                     (FileChannel/open  (.toPath mmap-file)
                                        (into-array [StandardOpenOption/APPEND]))
                     positions
                     (atom nil)
                     )


        _ (.addObject string-list "hello")
        _ (.addObject string-list "my world")]
    (is (= 2 (.lsize string-list)))
    (is (= "hello" (.readObject string-list 0)))
    (is (= "my world" (.readObject string-list 1)))
    ))






(comment
  (spit "/tmp/test.mmap" "")
  (def positions (atom []))
  (def my-list (string-list/->MmapStringList
                "/tmp/test.mmap"
                (FileChannel/open  (Paths/get (URI. "file:/tmp/test.mmap"))
                                   (into-array [StandardOpenOption/APPEND]))
                positions
                (atom nil)
                ))
  (.addObject my-list "super world")
  (.lsize my-list)
  (.readObject my-list 0)
  (.writeObject my-list 0 "")

  (.mmap my-list)

  @positions

  (.nth my-list 0)

  )
