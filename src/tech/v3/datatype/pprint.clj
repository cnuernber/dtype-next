(ns tech.v3.datatype.pprint
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.packing :as packing]))


;; pretty-printing utilities for matrices
(def ^:dynamic *number-format* "%.4G")


(defn format-num [x]
  (if (integer? x)
    (str x)
    (format *number-format* (double x))))


(defn format-object
  [x]
  (if (number? x)
    (format-num x)
    (str x)))


(defmulti reader-converter
  "Given a item that is of a datatype that is unprintable or that prints incorrectly
  return a new reader of a datatype that will print correctly (or just a reader of
  strings is fine).  This is sometimes called for iterables also."
  (fn [item]
    (when item
      (dtype-proto/elemwise-datatype item))))


(defmethod reader-converter :default
  [item]
  item)


(defn print-reader-data
  ^String [rdr & {:keys [formatter]
                  :or {formatter format-object}}]
  (let [rdr (reader-converter rdr)
        ^StringBuilder builder
        (->> (packing/unpack rdr)
             (dtype-proto/->reader)
             (reduce (fn [^StringBuilder builder val]
                       (.append builder
                                (formatter val))
                       (.append builder ", "))
                     (StringBuilder.)))]
    (.toString builder)))


(defn buffer->string
  (^String [buffer typename]
   (let [n-items (dtype-proto/ecount buffer)
         datatype (dtype-proto/elemwise-datatype buffer)
         format-str (if (> n-items 20)
                      "#%s<%s>%s\n[%s...]"
                      "#%s<%s>%s\n[%s]")]
     (format format-str
             typename
             (name datatype)
             [n-items]
             (-> (dtype-proto/sub-buffer buffer 0 (min 20 n-items))
                 (print-reader-data)))))
  (^String [buffer]
   (buffer->string buffer (.getCanonicalName (.getClass ^Object buffer)))))


(defmacro implement-tostring-print
  [typename]
  `(defmethod print-method ~typename
     [buf# w#]
     (.write ^java.io.Writer w# (.toString ^Object buf#))))
