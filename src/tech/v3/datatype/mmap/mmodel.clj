(ns tech.v3.datatype.mmap.mmodel
  "Mmap based on the MemorySegment jdk16 memory model pathway"
  (:require [tech.v3.datatype.ffi.native-buffer-mmodel :as nbuf-mmodel]
            [tech.v3.resource :as resource])
  (:import [jdk.incubator.foreign LibraryLookup CLinker FunctionDescriptor
            MemoryLayout LibraryLookup$Symbol MemorySegment]
           [java.nio.channels FileChannel$MapMode]
           [java.nio.file Path Paths]
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
         mseg (-> (MemorySegment/mapFile (->path fpath) 0 flen
                                         (case mmap-mode
                                           :read-only FileChannel$MapMode/READ_ONLY
                                           :read-write FileChannel$MapMode/READ_WRITE
                                           :private FileChannel$MapMode/PRIVATE))
                  (.share))
         nbuf (nbuf-mmodel/memory-segment->native-buffer mseg options)]
     (when resource-type
       (resource/track nbuf {:track-type resource-type
                             :dispose-fn #(.close mseg)}))
     nbuf))
  (^NativeBuffer [fpath] (mmap-file fpath {})))
