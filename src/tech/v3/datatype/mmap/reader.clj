(ns tech.v3.datatype.mmap.reader
  "Experimental namespace that reads data directly via mmap.  So far experimental
  results show that it is slower than using a buffered reader."
  (:require [tech.v3.datatype.mmap :as mmap]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.base :as dtype-base])
  (:import [java.io Reader]
           [java.nio.charset Charset CharsetDecoder CoderResult]
           [java.nio CharBuffer ByteBuffer]
           [java.nio.file Files Paths]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.datatype IOReader]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn ->charset
  ^Charset [charset]
  (cond
    (instance? Charset charset)
    charset
    (instance? String charset)
    (Charset/forName charset)))


(def ^{:private true
       :tag 'long}
  read-len (* 8 1024 1024))


(deftype ^:private MMapCharsetReader [^NativeBuffer nbuf
                                      ^CharsetDecoder decoder
                                      ^long bytesPerChar
                                      ^{:unsynchronized-mutable true
                                        :tag long} pos
                                      ^{:unsynchronized-mutable true
                                        :tag ByteBuffer} bbuf
                                      ^{:unsynchronized-mutable true} flushed]
  IOReader
  (doClose [_this])
  (doRead [_this char-ary off end]
    (let [len (- end off)]
      (cond
        (== 0 len)
        0
        flushed
        -1
        :else
        (let [char-buf (CharBuffer/wrap char-ary off len)
              ^CharBuffer char-buf (if (== 0 off)
                                    char-buf
                                    (.slice char-buf))]
          (loop [cur-offset (long (if flushed
                                    len
                                    (.position char-buf)))]
            (if (== len cur-offset)
              (.position char-buf)
              (let [[^ByteBuffer local-bbuf eof]
                    (if (and bbuf (.hasRemaining bbuf))
                      [bbuf false]
                      (let [available (- (.n-elems nbuf) pos)
                            buf-size (min available read-len)
                            ;;This call is time consuming
                            local-bbuf (-> (dtype-base/sub-buffer nbuf pos buf-size)
                                           (nio-buffer/native-buf->nio-buf
                                            {:resource-type nil}))]
                        (set! pos (+ pos buf-size))
                        [local-bbuf (== 0 available)]))
                    eof (boolean eof)]
                (set! bbuf local-bbuf)
                (let [code-result (.decode decoder bbuf char-buf eof)
                      ^CoderResult code-result
                      (if (and eof (.isUnderflow code-result))
                        (do
                          (set! flushed true)
                          (.flush decoder char-buf))
                        code-result)
                      _ (when (or (.isMalformed code-result)
                                  (.isError code-result)
                                  (.isUnmappable code-result))
                          (.throwException code-result))
                      cur-offset (long (if (or (.isOverflow code-result)
                                               (and (.isUnderflow code-result) eof))
                                         len
                                         (.position char-buf)))]
                  (recur cur-offset))))))))))


(deftype MMapReader [^NativeBuffer nbuf
                     ^{:unsynchronized-mutable true
                       :tag long} pos]
  IOReader
  (doClose [_this])
  (doRead [_this char-ary off end]
    (let [available (- (.n-elems nbuf) pos)
          len (min available (- end off))]
      (cond
        (== available 0)
        -1
        (== len 0)
        0
        :else
        (let [unsafe (native-buffer/unsafe)]
          (dotimes [idx len]
            (aset char-ary (+ off idx)
                  (unchecked-char
                   (.getByte unsafe (+ (.address nbuf) pos idx)))))
          (set! pos (+ pos len))
          len)))))


(defn native-buf-reader
  ^Reader [^NativeBuffer nbuf charset]
  (if charset
    (let [charset (->charset charset)
          charsetDecoder (.newDecoder charset)
          bytesPerChar (max 1 (Math/ceil (/ 1.0
                                            (.averageCharsPerByte charsetDecoder))))]
      (-> (MMapCharsetReader. nbuf charsetDecoder bytesPerChar 0 nil false)
          (.makeReader nbuf)))
    (-> (MMapReader. nbuf 0)
        (.makeReader nbuf))))


(defn mmap-reader
  (^Reader [fname charset mmap-options]
   (native-buf-reader (mmap/mmap-file fname mmap-options) charset))
  (^Reader [fname charset]
   (mmap-reader fname charset nil)))


(comment
  (require 'clojure.java.io)
  (defn test-read-file
    [^Reader rdr]
    (with-open [rdr rdr]
      (let [char-buf (char-array 4096)]
        (loop [n-read (.read rdr char-buf)
               total-read 0]
          (if (== -1 n-read)
            total-read
            (recur (.read rdr char-buf) (+ total-read n-read)))))))

  (time (test-read-file (clojure.java.io/reader "/home/chrisn/Downloads/yellow_tripdata_2016-01.csv")))
  ;;3625.28ms
  (time (test-read-file (mmap-reader "/home/chrisn/Downloads/yellow_tripdata_2016-01.csv" nil)))
  ;;4500ms
  (time (test-read-file (Files/newBufferedReader (Paths/get "/home/chrisn/Downloads/yellow_tripdata_2016-01.csv" (into-array String [])))))
  ;;3692ms
  )
