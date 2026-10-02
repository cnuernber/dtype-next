(ns tech.v3.datatype.mmap.larray
  "Bindings to an mmap pathway (provided by xerial.larray.mmap)."
  (:require [clojure.java.io :as io]
            [tech.v3.resource :as resource]
            [clojure.tools.logging :as log]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.protocols :as dtype-proto])
  (:import [xerial.larray.mmap MMapMode]
           [tech.v3.datatype MMapBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]))


(set! *warn-on-reflection* true)


(defn mmap-file
  "Memory map a file returning a native buffer.  fpath must resolve to a valid
   java.io.File.
  Options
  * :resource-type - maps to tech.v3.resource `:track-type`, defaults to :auto.
  * :mmap-mode
    * :read-only - default - map the data as shared read-only.
    * :read-write - map the data as shared read-write.
    * :private - map a private copy of the data and do not share."
  (^NativeBuffer [fpath {:keys [resource-type mmap-mode endianness]
                         :or {resource-type :auto
                              mmap-mode :read-only}}]
   (let [file (io/file fpath)
         _ (when-not (.exists file)
             (throw (Exception. (format "%s not found" fpath))))
         resource-type (resource/normalize-track-type resource-type)
         ;;Mapping to read-only means pages can be shared between processes
         map-buf (MMapBuffer. file (case mmap-mode
                                     :read-only MMapMode/READ_ONLY
                                     :read-write MMapMode/READ_WRITE
                                     :private MMapMode/PRIVATE))
         endianness (or endianness (dtype-proto/platform-endianness))]
     ;;The unmapper does not reference map-buf so it is safe for gc tracking.  The
     ;;returned native buffer holds map-buf as its parent keeping the mapping alive.
     (let [^Runnable unmapper (.-unmapper map-buf)]
       (resource/track map-buf
                       {:dispose-fn #(do (log/debugf "closing %s" fpath)
                                         (.run unmapper))
                        :track-type resource-type}))
     (native-buffer/wrap-address (.address map-buf) (.mapSize map-buf) :int8
                                 endianness map-buf)))
  (^NativeBuffer [fpath]
   (mmap-file fpath {})))
