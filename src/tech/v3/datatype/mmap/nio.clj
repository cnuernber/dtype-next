(ns tech.v3.datatype.mmap.nio
  (:require [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.resource :as resource])
  (:import [java.nio.channels FileChannel FileChannel$MapMode]
           [java.nio.file Files Paths StandardOpenOption OpenOption]))

(set! *warn-on-reflection* true)


(defn mmap-file
  [fpath options]
  (let [mmap-mode (:mmap-mode options :read-only)
        fpath (Paths/get fpath (make-array String 0))
        n-bytes (Files/size fpath)
        ;;Short sighted own-goal brought to you by Java (tm).
        _ (errors/when-not-errorf
           (< n-bytes Integer/MAX_VALUE)
           "\"%s\" is larger (%d) than max allowed size (%d)"
           (str fpath) n-bytes Integer/MAX_VALUE)
        channel-options (into-array OpenOption
                                    (case mmap-mode
                                      :read-only [StandardOpenOption/READ]
                                      :read-write [StandardOpenOption/READ
                                                   StandardOpenOption/WRITE]
                                      :private [StandardOpenOption/READ
                                                StandardOpenOption/WRITE]))
        file-channel
        (Files/newByteChannel
         fpath
         ^"[Ljava.nio.file.OpenOption;" channel-options)
        map-mode (case mmap-mode
                   :read-only FileChannel$MapMode/READ_ONLY
                   :read-write FileChannel$MapMode/READ_WRITE
                   :private FileChannel$MapMode/PRIVATE)
        byte-buf (.map ^FileChannel file-channel ^FileChannel$MapMode map-mode
                       0 n-bytes)
        n-buf (-> (dtype-base/as-native-buffer byte-buf)
                  (native-buffer/set-parent nil))
        res-type (:resource-type options :auto)]
    (if-not (nil? res-type)
      (resource/track n-buf {:track-type res-type
                             :dispose-fn #(do
                                            ;;force a reference to byte-buf
                                            (identity byte-buf)
                                            ;;close the file channel
                                            (.close file-channel))})
      (native-buffer/set-parent n-buf byte-buf))))
