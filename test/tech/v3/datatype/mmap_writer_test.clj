(ns tech.v3.datatype.mmap-writer-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.mmap-writer :as mmap-writer]
            [tech.v3.resource :as resource]))

(deftest test-add-read
  (let [fpath
        (resource/stack-resource-context
         (let [mmap-file (mmap-writer/temp-mmap-writer)]
           (.writeData mmap-file "testtesttest")
           (is (= 12 (.lsize mmap-file)))
           (is (= "testtesttest" (String. (dtype/->array
                                   (dtype/->buffer mmap-file)))))
           (.fpath mmap-file)))]
    (is (= false (.delete (java.io.File. fpath))))))






(comment
  (spit "/tmp/test.mmap" "")
  (def positions (atom []))
  (def my-list (mmap-list/->MmapList
                #(String. %)
                #(.getBytes %)
                :string
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
