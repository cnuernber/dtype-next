(ns tech.v3.datatype.mmap-string-list-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer [deftest is]]
            [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.mmap-string-list :as string-list])
  (:import [tech.v3.datatype ObjectBuffer PrimitiveList]))




(deftest test-add-read
  (let [mmap-file (.getPath (java.io.File/createTempFile "strings" ".mmap") )
        positions (atom [])
        string-list (string-list/->MmapStringList
                     mmap-file
                     (io/output-stream mmap-file :append true)
                     positions
                     (atom nil)
                     )]


    (.addObject string-list "test")
    (is (= 1 (.lsize string-list)))
    (is (= "test" (.readObject string-list 0)))
    ))

(deftest test-add-read-varoius
  (let [mmap-file (.getPath (java.io.File/createTempFile "strings" ".mmap") )
        positions (atom [])
        string-list (string-list/->MmapStringList
                     mmap-file
                     (io/output-stream mmap-file :append true)
                     positions
                     (atom nil)
                     )


        _ (.addObject string-list "hello")
        _ (.addObject string-list "my world")]
    (is (= 2 (.lsize string-list)))
    (is (= "hello" (.readObject string-list 0)))
    (is (= "my world" (.readObject string-list 1)))
    ))






(
  (comment
    (spit "/tmp/test.mmap" "")
    (def positions (atom []))
    (def my-list (string-list/->MmapStringList  "/tmp/test.mmap"
                                                (io/output-stream "/tmp/test.mmap")
                                                positions
                                                (atom nil)
                                                ))
    (.addObject my-list "super world")
    (.lsize my-list)
    (.readObject my-list 0)
    (.writeObject my-list 0 "")

    (.mmap my-list)

    @positions

    (.lsize my-list)
    (.nth my-list 1))

  )
