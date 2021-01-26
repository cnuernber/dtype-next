(ns tech.v3.datatype.mmap-ffi
  (:require [tech.v3.datatype.ffi :as ffi]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.resource :as resource])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]))


;;OPENPROT
(def ^{:tag 'long} O_RDONLY 0)
(def ^{:tag 'long} O_WRONLY 1)
(def ^{:tag 'long} O_RDWR 2)

(ffi/def-ffi-fn nil open
  "Open a file"
  :int32
  [:pointer fname]
  [:int32 openprot])


(ffi/def-ffi-fn nil close
  "Close an opened file"
  :int32
  [:int32 fhandle])

;;Unix-based mmap.  Windows will have to wait for someone
;;with a windows machine

(def ^{:tag 'long} PROT_READ   0x1) ;; Page can be read.
(def ^{:tag 'long} PROT_WRITE  0x2) ;; Page can be written.
(def ^{:tag 'long} PROT_EXEC   0x4) ;; Page can be executed.
(def ^{:tag 'long} PROT_NONE   0x0) ;; Page can not be accessed.

(def ^{:tag 'long} MAP_SHARED  0x01)   ;; Share changes.
(def ^{:tag 'long} MAP_PRIVATE 0x02)   ;; Changes are private.
(def ^{:tag 'long} MAP_ANONYMOUS 0x20) ;; Do not use a file backing


(ffi/def-ffi-fn nil mmap
  "mmap a file descriptor, returning a pointer to file-backed"
  (ffi/ptr-int-type)
  [(ffi/ptr-int-type) address]
  [(ffi/size-t-type) length]
  [:int32 protect]
  [:int32 flags]
  [:int32 filedes]
  [(ffi/size-t-type) offset])


(ffi/def-ffi-fn nil munmap
  "unmap a region"
  :int32
  [(ffi/ptr-int-type) address]
  [(ffi/size-t-type) length])


(defn mmap-file
  "Memory map a file returning a native buffer.  fpath must resolve to a valid
   java.io.File.
  Options
  * :resource-type - maps to tech.v3.resource `:track-type`, defaults to auto.
  * :mmap-mode
    * :read-only - default - map the data as shared read-only.
    * :read-write - map the data as shared read-write."
  (^NativeBuffer [fpath {:keys [resource-type mmap-mode endianness]
                         :or {resource-type :auto
                              mmap-mode :read-only}}]
   (let [fhandle (long (open fpath (case mmap-mode
                                     :read-only O_RDONLY
                                     :read-write O_RDWR)))
         _ (errors/when-not-errorf
            (not= -1 fhandle)
            "Open system call failed--does file exist?")
         flen (.length (java.io.File. fpath))
         addr (long (mmap 0 flen MAP_SHARED (case mmap-mode
                                              :read-only PROT_READ
                                              :read-write (bit-or
                                                           PROT_READ
                                                           PROT_WRITE))
                          fhandle 0))
         _ (errors/when-not-error
            (not= -1 addr)
            "MMap system call failed")
         ;;not needed once file is mapped
         _ (close fhandle)
         nbuf (native-buffer/wrap-address addr flen :int8 endianness nil)]
     (when resource-type
       (resource/track nbuf {:track-type resource-type
                             :dispose-fn #(munmap addr flen)}))
     nbuf))
  (^NativeBuffer [fpath] (mmap-file fpath {})))


;; (def ^{:tag 'long} MS_ASYNC 1) ;; Sync memory asynchronously.
;; (def ^{:tag 'long} MS_SYNC  4) ;; Synchronous memory sync.
;; (def ^{:tag 'long} MS_INVALIDATE 2) ;; Invalidate the caches.
