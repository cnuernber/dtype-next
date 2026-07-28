(ns tech.v3.datatype.ffi.libpath
  (:require [ham-fisted.lazy-noncaching :as lznc]))


(defn is-m1-mac
  []
  (= "aarch64" (System/getProperty "os.arch")))


(defn library-paths
  [^String libname]
  (if (is-m1-mac)
    (if (or (.contains libname "\\")
            (.contains libname "/"))
      [libname]
      [libname (str"/opt/homebrew/lib/lib" libname ".dylib")])
    [libname]))


(defn load-library!
  [loader valid-check libname]
  (let [loaded (->> (library-paths libname)
                      (eduction (map (fn [libname]
                                       (try
                                         (loader (str libname))
                                         (catch Throwable e e)))))
                      (seq))
          valid (first (filter valid-check loaded))]
    (if valid valid (throw (first loaded)))))


(defn resolve-via-system-path
  [libname]
  (let [libname (str libname)]
    (if (.startsWith libname "/")
      libname
      (->> (.split (str (System/getenv "PATH")) ":")
           (lznc/map (fn [prefix]
                       (let [prefix (str prefix)
                             prefix (if-not (.endsWith prefix "/") (str prefix "/") prefix)]
                         (str prefix (System/mapLibraryName libname)))))
           (lznc/filter #(.exists (java.io.File. (str %))))
           (first)))))


(comment
  (is-m1-mac)
  (library-paths "lz4")
  (library-paths "/usr/lib/liblz4.dylib")
  )
