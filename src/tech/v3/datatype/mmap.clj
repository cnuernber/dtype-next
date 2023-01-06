(ns tech.v3.datatype.mmap
  (:require [tech.v3.datatype.native-buffer]
            [tech.v3.datatype.graal-native :as graal-native]
            [tech.v3.datatype.errors :as errors]
            [clojure.tools.logging :as log])
  (:import [tech.v3.datatype.native_buffer NativeBuffer]))


(defonce ^:private mmap-fn* (atom nil))


(defn set-mmap-impl!
  "Set the system mmap implementation to use.  There are three provided under the
  mmap directory -
  * `mmodel` - Use the JDK-17 memory model.  You have to have the module enabled.
  * `larray` - Use LArray mmap - default if available.

  If unset then the system will try to load the jdk-17 namespace, then larray."
  [mmap-fn]
  (reset! mmap-fn* mmap-fn))


(defn jdk-major-version
  ^long []
  (let [jv (System/getProperty "java.version")]
    (try
      (-> (re-find #"^\d+" jv)
          (Long/parseLong))
      (catch Exception _e 8))))


(defn- resolve-mmap-fn
  []
  (swap! mmap-fn*
         (fn [existing-fn]
           (if existing-fn
             existing-fn
             (graal-native/if-defined-graal-native
              (errors/throwf "Automatic resolution of mmap pathways is disabled for graal native. Please use 'tech.3.datatype.mmap/set-mmap-impl!' prior to calling mmap-file.")
              (let [jdk-19-fn
                    (when (>= (jdk-major-version) 19)
                      (try
                        (requiring-resolve 'tech.v3.datatype.mmap.mmodel-jdk19/mmap-file)
                        (catch Throwable _e
                          (log/warn "Failed to activate JDK-19 mmodel pathway; attempting jdk-17."))))]
                (if jdk-19-fn
                  jdk-19-fn
                  (let [jdk-17-fn
                        (when (>= (jdk-major-version) 17)
                          (try
                            (requiring-resolve 'tech.v3.datatype.mmap.mmodel/mmap-file)
                            (catch Throwable _e
                              (log/warn "Failed to activate JDK-17 mmodel pathway; falling back to larray."))))]
                    (if jdk-17-fn
                      jdk-17-fn
                      (try
                        (requiring-resolve 'tech.v3.datatype.mmap.larray/mmap-file)
                        (catch Throwable e
                          (log/warnf "Mmap unavailble as larray failed to load: %s" e)
                          :failed)))))))))))


(defn mmap-enabled?
  []
  (graal-native/if-defined-graal-native
   (not (nil? @mmap-fn*))
   (do (resolve-mmap-fn)
       (not (or (keyword? @mmap-fn*)
                (nil? @mmap-fn*))))))


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
   (when-not (mmap-enabled?)
     (throw (Exception. "MMap is disabled")))
   ((resolve-mmap-fn) fpath options))
  (^NativeBuffer [fpath]
   (mmap-file fpath {})))
