(ns tech.v3.datatype.mmap.mmodel-jdk
  "Mmap based on the MemorySegment JDK FFM API - finalized in JDK 22 (JEP 454), available in LTS starting with JDK 25."
  (:require [tech.v3.datatype.ffi.native-buffer-mmodel-jdk :as nbuf-mmodel]
            [tech.v3.resource :as resource])
  (:import [java.lang.foreign MemorySegment Arena]
           [java.nio.channels FileChannel$MapMode FileChannel]
           [java.nio.file Path Paths StandardOpenOption OpenOption]
           [tech.v3.datatype.native_buffer NativeBuffer]))


(set! *warn-on-reflection* true)


(defn- ->path
  ^Path [item]
  (if (instance? Path item)
    item
    (Paths/get item (into-array String []))))


(defn mmap-file
  "Memory map a file returning a native buffer.  fpath must resolve to a valid
   java.io.File.
  Options
  * :resource-type - maps to tech.v3.resource `:track-type`, defaults to auto.
  * :mmap-mode
    * :read-only - default - map the data as shared read-only.
    * :read-write - map the data as shared read-write.
  * :map-len - Override the file's length to provide a mapping length"
  (^NativeBuffer [fpath {:keys [resource-type mmap-mode]
                         :or {resource-type :auto
                              mmap-mode :read-only}
                         :as options}]
   (let [flen (long (:map-len options (.length (java.io.File. (str fpath)))))
         channel (FileChannel/open (->path fpath)
                                   (into-array OpenOption
                                               (case mmap-mode
                                                 :read-only
                                                 [StandardOpenOption/READ]
                                                 :read-write
                                                 [StandardOpenOption/READ
                                                  StandardOpenOption/WRITE]
                                                 :private
                                                 [StandardOpenOption/READ])))
         rscope (Arena/ofShared)
         mseg (.map channel
                    (case mmap-mode
                      :read-only FileChannel$MapMode/READ_ONLY
                      :read-write FileChannel$MapMode/READ_WRITE
                      :private FileChannel$MapMode/PRIVATE)
                    0 flen
                    rscope)
         nbuf (nbuf-mmodel/memory-segment->native-buffer mseg options)]
     (when resource-type
       (resource/track nbuf {:track-type resource-type
                             :dispose-fn #(do (.close rscope)
                                              (.close channel))}))
     nbuf))
  (^NativeBuffer [fpath] (mmap-file fpath {})))
