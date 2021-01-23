(ns tech.v3.datatype.mmap
  (:require [clojure.tools.logging :as log])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]))


(def mmap-fn* (delay (try
                       (requiring-resolve 'tech.v3.datatype.mmap-larray/mmap-file)
                       (catch Throwable e
                         (log/info "Failed to require larray-based mmap -
falling back to jdk16+ ffi-based mmap")
                         (requiring-resolve 'tech.v3.datatype.mmap-ffi/mmap-file)))))


(defn mmap-file
  "Memory map a file returning a native buffer.  fpath must resolve to a valid
   java.io.File.
  Options
  * :resource-type - maps to tech.v3.resource `:track-type`, defaults to :auto.
  * :mmap-mode
    * :read-only - default - map the data as shared read-only.
    * :read-write - map the data as shared read-write.
    * :private - map a private copy of the data and do not share."
  (^NativeBuffer [fpath options]
   (@mmap-fn* fpath options))
  (^NativeBuffer [fpath]
   (mmap-file fpath {})))
