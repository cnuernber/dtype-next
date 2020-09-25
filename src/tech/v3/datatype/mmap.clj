(ns tech.v3.datatype.mmap
  "Bindings to an mmap pathway (provided by xerial.larray.mmap)."
  (:require [clojure.java.io :as io]
            [tech.resource :as resource]
            [clojure.tools.logging :as log]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [xerial.larray.mmap MMapBuffer MMapMode]
           [xerial.larray.buffer UnsafeUtil]
           [sun.misc Unsafe]))


(set! *warn-on-reflection* true)


(defn mmap-file
  "Memory map a file returning a native buffer.  fpath must resolve to a valid
   java.io.File.
  Options
  * :resource-type - Chose the type of resource management to use with the returned
     value:
     * `:gc` - default - The mmaped file will be cleaned up when the garbage collection system
         decides to collect the returned native buffer.
     * `:stack` mmap-file call must be wrapped in a call to
        tech.resource/stack-resource-context and will be cleaned up when control leaves
        the form.

     * `nil` - The mmaped file will never be cleaned up.

  * :mmap-mode
    * :read-only - default - map the data as shared read-only.
    * :read-write - map the data as shared read-write.
    * :private - map a private copy of the data and do not share."
  ([fpath {:keys [resource-type mmap-mode endianness]
           :or {resource-type :gc
                mmap-mode :read-only}}]
   (let [file (io/file fpath)
         _ (when-not (.exists file)
             (throw (Exception. (format "%s not found" fpath))))
         ;;Mapping to read-only means pages can be shared between processes
         map-buf (MMapBuffer. file (case mmap-mode
                                     :read-only MMapMode/READ_ONLY
                                     :read-write MMapMode/READ_WRITE
                                     :private MMapMode/PRIVATE))
         endianness (or endianness (dtype-proto/platform-endianness))]
     ;;the mmap library has it's own gc-based cleanup system that works fine.
     (when-not (= resource-type :gc)
       (resource/track map-buf
                       #(do (log/debugf "closing %s" fpath) (.close map-buf))
                       resource-type))
     (native-buffer/wrap-address (.address map-buf) (.size map-buf) :int8
                                 endianness map-buf)))
  ([fpath]
   (mmap-file fpath {})))
