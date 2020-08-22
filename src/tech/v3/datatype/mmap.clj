(ns tech.v3.datatype.mmap
  (:require [clojure.java.io :as io]
            [tech.resource :as resource]
            [clojure.tools.logging :as log]
            [tech.v3.datatype.native-buffer :as native-buffer])
  (:import [xerial.larray.mmap MMapBuffer MMapMode]
           [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe]))


(set! *warn-on-reflection* true)


(defn unsafe
  ^Unsafe []
  UnsafeUtil/unsafe)


(defn mmap-file
  "Memory map a file returning a native buffer.  fpath must resolve to a valid
   java.io.File.
  Options
  * :resource-type - Chose the type of resource management to use with the returned
     value:
     * `:stack` - default - mmap-file call must be wrapped in a call to
        tech.resource/stack-resource-context and will be cleaned up when control leaves
        the form.
     * `:gc` - The mmaped file will be cleaned up when the garbage collection system
         decides to collect the returned native buffer.
     * `nil` - The mmaped file will never be cleaned up.

  * :mmap-mode
    * :read-only - default - map the data as shared read-only.
    * :read-write - map the data as shared read-write.
    * :private - map a private copy of the data and do not share."
  ([fpath {:keys [resource-type mmap-mode]
           :or {resource-type :stack
                mmap-mode :read-only}}]
   (let [file (io/file fpath)
         _ (when-not (.exists file)
             (throw (Exception. (format "%s not found" fpath))))
         ;;Mapping to read-only means pages can be shared between processes
         map-buf (MMapBuffer. file (case mmap-mode
                                     :read-only MMapMode/READ_ONLY
                                     :read-write MMapMode/READ_WRITE
                                     :private MMapMode/PRIVATE))]
     (if resource-type
       (resource/track map-buf
                       #(do (log/debugf "closing %s" fpath) (.close map-buf))
                       resource-type)
       (log/debugf "No resource type specified for mmaped file %s" fpath))
     (native-buffer/->NativeBuffer (.address map-buf) (.size map-buf) :int8)))
  ([fpath]
   (mmap-file fpath {})))
