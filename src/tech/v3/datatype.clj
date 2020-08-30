(ns tech.v3.datatype
  (:require [tech.v3.datatype.array-buffer]
            [tech.v3.datatype.native-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.copy-make-container :as dtype-copy-make-container]
            [tech.v3.datatype.clj-range]
            [tech.v3.datatype.list]
            [tech.v3.datatype.functional]
            [tech.v3.datatype.copy-raw-to-item]
            [tech.v3.datatype.primitive]
            [tech.v3.datatype.nio-buffer]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]))


(export-symbols tech.v3.datatype.base
                elemwise-datatype
                ecount
                ->io
                ->reader
                ->writer
                ->array-buffer
                ->native-buffer
                sub-buffer
                get-value
                set-value!)

(export-symbols tech.v3.datatype.copy-make-container
                make-container
                copy!)

(defn copy-raw->item!
  "Copy raw data into a buffer.  Data may be a sequence of numbers or a sequence
  of containers.  Data will be coalesced into the buffer.  Returns a tuple of:
  [buffer final-offset]."
  ([raw-data buffer options]
   (dtype-proto/copy-raw->item! raw-data buffer 0 options))
  ([raw-data buffer]
   (dtype-proto/copy-raw->item! raw-data buffer 0 {})))


(defn coalesce!
  "Coalesce data from a raw sequence of things into a contiguous buffer.
  Returns the buffer; uses the copy-raw->item! pathway."
  [raw-data buffer]
  (first (copy-raw->item! raw-data buffer)))


(defn ->vector
  "Convert a datatype thing to a vector"
  [item]
  (vec (->reader item)))
