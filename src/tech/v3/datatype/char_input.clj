(ns tech.v3.datatype.char-input
  "Efficient ways to read files via the java.io.Reader interface.  You can read a file
  into an iterator of (fixed rotating) character buffers, create a new and much
  faster reader-like interface from the character buffers and parse a csv/tsv type
  file with an interface that is mostly compatible with but far faster than clojure.data.csv.

  Files are by default read by a separate thread into character arrays and those arrays are
  then processed.  For details around the threading system see [[tech.v3.parallel.queue-iter]].

  CSV parsing is broken up into two parts.  The first is reading a file and creating an iterator
  of char[] buffers.  The second is parsing an iterator of char[] buffers.  As mentioned earlier
  we further move the file->char buffer parsing off onto an offline thread.

  This design is meant to be easy to experiment with.  It could be that an mmap-based pathway
  allows us to read the file faster, it could be that parsing into blocks of rows as opposed
  to char[]s in an offline thread is faster, etc.

  Overall parsing many csv's in parallel will a better strategy than using many threads to
  make parsing a single csv incrementally faster so this current design keeps the memory
  requirements for a single csv quite low while still gaining the majority of meaningful
  performance benefits.

  Supporting Java classes:

  * [CharBuffer.java](https://github.com/cnuernber/dtype-next/blob/master/java/tech/v3/datatype/CharBuffer.java) -
    StringBuilder-like class that implements whitespace trimming, clear, and nil empty strings.
  * [CharReader.java](https://github.com/cnuernber/dtype-next/blob/master/java/tech/v3/datatype/CharReader.java) -
    A java.io.Reader-like class that only correctly implements single-character unread but contains the
    tight loops required to efficiently parse a CSV file."
  (:require [clojure.java.io :as io]
            [tech.v3.parallel.queue-iter :as queue-iter]
            [com.github.ztellman.primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype CharReader UnaryPredicate UnaryPredicates$LongUnaryPredicate
            CharBuffer]
           [java.io Reader StringReader]
           [java.util Iterator Arrays ArrayList List NoSuchElementException]
           [java.lang AutoCloseable]
           [org.roaringbitmap RoaringBitmap]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(deftype ^:private CharBufIter [^objects buffers
                                ^Reader reader
                                ^{:unsynchronized-mutable true
                                  :tag long} next-buf-idx
                                ^{:unsynchronized-mutable true
                                  :tag chars} cur-buf
                                ^long n-buffers
                                close-reader?]
  Iterator
  (hasNext [this] (not (nil? cur-buf)))
  (next [this]
    (let [retval cur-buf
          idx (rem next-buf-idx n-buffers)
          ^chars buf (aget buffers idx)
          nchars (.read reader ^chars buf)
          buf (cond
                (pmath/== nchars (alength buf))
                buf
                (pmath/== nchars -1)
                nil
                :else
                (Arrays/copyOf buf nchars))]
      (set! next-buf-idx (unchecked-inc next-buf-idx))
      (set! cur-buf buf)
      retval))
  AutoCloseable
  (close [this]
    (when close-reader?
      (.close reader))))


(defn- ->reader
  ^Reader [item]
  (cond
    (instance? Reader item)
    item
    (string? item)
    (StringReader. item)
    :else
    (io/reader item)))


(defn reader->char-buf-iter
  "Given a reader, return an iterator of a sequence of character buffers.  This rotates
  through a fixed number of buffers under the covers so you need to be cognizant of the number
  of actual buffers that you want to have present in memory.

  Options:

  * `:n-buffers` - Number of buffers to use.  Defaults to 8 - if this number is too small
  then buffers in flight will get overwritten.
  * `:bufsize` - Size of each buffer - defaults to 2048.  Small improvements are sometimes
  seen with larger or smaller buffers.
  * `:close-reader?` - When true, close input reader when finished.  Defaults to true."
  ^Iterator [rdr & [options]]
  (let [rdr (->reader rdr)
        n-buffers (long (get options :n-buffers 8))
        bufsize (long (get options :bufsize 2048))
        buffers (object-array (repeatedly n-buffers #(char-array bufsize)))
        ^chars buf (aget buffers 0)
        nchars (.read rdr buf)
        buf (cond
              (pmath/== nchars bufsize)
              buf
              (pmath/== nchars -1)
              nil
              :else
              (Arrays/copyOf buf nchars))]
    (CharBufIter. buffers rdr 1 buf n-buffers (get options :close-reader? true))))


(defn- ->character
  [v]
  (cond
    (char? v)
    v
    (string? v)
    (do
      (when-not (== 1 (.length ^String v))
        (throw (Exception.
                (format "Only single character separators allowed: - \"%s\""
                        v))))
      (first v))
    (number? v)
    (unchecked-char v)))


(defn reader->char-reader
  "Given a reader, return a CharReader which presents some of the same interface
  as a pushbackreader but is only capable of pushing back 1 character.

  Options:

  Options are passed through mainly unchanged to queue-iter and to
  [[reader->char-buf-iter]].

  * `:async?` - default to true - reads the reader in an offline thread into character
     buffers."
  ^CharReader [rdr & [options]]
  (let [quote (->character (get options :quote \"))
        separator (->character (get options :separator \,))
        trim-leading? (get options :trim-leading-whitespace? true)
        async? (get options :async? true)
        options (if async?
                  ;;You need some number more buffers than queue depth else buffers will be
                  ;;overwritten during processing.  I calculate you need at least 3  - one
                  ;;in the source thread, one for the iterator's next value and one that
                  ;;the system is parsing.  I added one more for safety.
                  (assoc options
                         :queue-depth 12
                         :n-buffers 18
                         :bufsize (get options :bufsize 8192)))
        src-iter (reader->char-buf-iter rdr options)
        src-iter (if async?
                   (queue-iter/queue-iter src-iter options)
                   src-iter)]
    (CharReader. src-iter quote separator)))


(def ^{:private true :tag 'long :const true} EOF CharReader/EOF)
(def ^{:private true :tag 'long :const true} QUOT CharReader/QUOT)
(def ^{:private true :tag 'long :const true} SEP CharReader/SEP)
(def ^{:private true :tag 'long :const true} EOL CharReader/EOL)

(defrecord ^:private RowRecord [row ^long tag])


(defn- empty-list?
  [^List data]
  (or (nil? data)
      (== 0 (.size data))
      (and (== 1 (.size data))
           (or (nil? (.get data 0))
               (.equals "" (.get data 0))))))

(def ^{:private true
       :tag UnaryPredicate}
  true-unary-predicate
  (reify UnaryPredicates$LongUnaryPredicate
    (unaryLong [this arg]
      true)))


(defn- read-row
  ^RowRecord [^CharReader rdr ^CharBuffer sb ^ArrayList row
              ^UnaryPredicate filter-fn]
  (.clear row)
  (let [tag (long (loop [tag (.csvRead rdr sb)
                         col-idx 0]
                    (if-not (or (== tag EOL)
                                (== tag EOF))
                      (do
                        (when (== tag SEP)
                          (when (.unaryLong filter-fn col-idx)
                            (.add row (.toString sb)))
                          (.clear sb))
                        (recur (long (if (== tag SEP)
                                       (.csvRead rdr sb)
                                       (.csvReadQuote rdr sb)))
                               (if (== tag SEP)
                                 (unchecked-inc col-idx)
                                 col-idx)))
                      (do
                        (when (.unaryLong filter-fn col-idx)
                          (.add row (.toString sb)))
                        (.clear sb)
                        tag))))
        new-row (.clone row)]
    (.clear row)
    (when-not (and (pmath/== tag EOF)
                   (empty-list? new-row))
      (RowRecord. new-row tag))))


(deftype ^:private CSVReadIter [^CharReader rdr
                                ^CharBuffer sb
                                ^ArrayList row-builder
                                ^{:unsynchronized-mutable true
                                  :tag RowRecord} cur-row
                                close?
                                ^UnaryPredicate column-whitelist]
  Iterator
  (hasNext [this] (not (nil? cur-row)))
  (next [this]
    (let [retval cur-row
          next-row (if (pmath/== (.tag cur-row) EOF)
                     nil
                     (read-row rdr sb row-builder column-whitelist))]
      (set! cur-row next-row)
      (when (and (nil? next-row) close?)
        (.close this))
      (.row retval)))
  AutoCloseable
  (close [this]
    (when (instance? AutoCloseable (.buffers rdr))
      (.close ^AutoCloseable (.buffers rdr)))))


(defn read-csv
  "Read a csv into a row iterator.  Parse algorithm the same as clojure.data.csv although
  this returns an iterator and each row is an ArrayList as opposed to a persistent
  vector.  To convert a java.util.List into something with the same equal and hash semantics
  of a persistent vector use either `tech.v3.datatype.ListPersistentVector` or `vec`.  To
  convert an iterator to a sequence use iterator-seq.

  The iterator returned derives from AutoCloseable and it will terminate the iteration and
  close the underlying iterator (and join the async thread) if (.close iter) is called.

  For a drop-in but much faster replacement to clojure.data.csv use [[read-csv-compat]].

  Options:

  * `:async?` - Defaults to true - read the file into buffers in an offline thread.  This
     speeds up reading larger files 1MB+ by about 25%.
  * `:separator` - Field separator - defaults to \\,.
  * `:quote` - Quote specifier - defaults to \\..
  * `:close-reader?` - Close the reader when iteration is finished - defaults to true.
  * `:column-whitelist` - Sequence of allowed column names.
  * `:column-blacklist` - Sequence of dis-allowed column names.  When conflicts with
     `:column-whitelist` then `:column-whitelist` wins.
  * `:trim-leading-whitespace?` - When true, leading spaces are ignored.  Defaults to true.
  * `:trim-trailing-whitespace?` - When true, trainling spaces and tabs are ignored.  Defaults
     to true
  * `:nil-empty-values?` - When true, empty strings are elided entirely and returned as nil
     values. Defaults to true."
  ^Iterator [input & [options]]
  (let [rdr (reader->char-reader input options)
        sb (CharBuffer. (get options :trim-leading-whitespace? true)
                        (get options :trim-trailing-whitespace? true)
                        (get options :nil-empty-values? true))
        row (ArrayList.)
        nil-empty? (get options :nil-empty-values? true)
        next-row (read-row rdr sb row true-unary-predicate)
        ^RoaringBitmap column-whitelist
        (when (or (contains? options :column-whitelist)
                  (contains? options :column-blacklist))
          (let [whitelist (when-let [data (get options :column-whitelist)]
                            (set data))
                blacklist (when-let [data (get options :column-blacklist)]
                            (set/difference (set data) (or whitelist #{})))
                indexes
                (->> (.row next-row)
                     (map-indexed
                      (fn [col-idx cname]
                        (when (or (and whitelist (whitelist cname))
                                  (and blacklist (not (blacklist cname))))
                          col-idx)))
                     (remove nil?)
                     (seq))
                bmp (RoaringBitmap.)]
            (.add bmp (int-array indexes))
            bmp))
        ^UnaryPredicate col-pred (if column-whitelist
                                   (reify UnaryPredicates$LongUnaryPredicate
                                     (unaryLong [this arg]
                                       (.contains column-whitelist (unchecked-int arg))))
                                   true-unary-predicate)]
    (when column-whitelist
      (let [^List cur-row (.row next-row)
            nr (.size cur-row)
            dnr (dec nr)]
        (dotimes [idx nr]
          (let [cur-idx (- dnr idx)]
            (when-not (.contains column-whitelist cur-idx)
              (.remove cur-row (unchecked-int cur-idx)))))))
    (CSVReadIter. rdr sb row next-row (get options :close-reader? true)
                  col-pred)))


(defn read-csv-compat
  "Read a csv returning a clojure.data.csv-compatible sequence.  For options
  see [[read-csv]]."
  [input & options]
  (let [options (->> (partition 2 options)
                     (map vec)
                     (into {}))]
    (->> (read-csv input options)
         (iterator-seq)
         (map vec))))


(comment
  (require '[clojure.java.io :as io])
  (require '[criterium.core :as crit])
  (def srcpath "../../tech.all/tech.ml.dataset/test/data/issue-292.csv")


  (defn read-all-reader
    [^Reader rdr]
    (loop [data (.read rdr)
           n-read 0]
      (if (== -1 data)
        n-read
        (recur (.read rdr) (unchecked-inc n-read)))))


  (defn read-all-cbuf
    [^Reader rdr]
    (let [cbuf (char-array 2048)]
      (loop [data (.read rdr cbuf)
             n-read 0]
        (if (== data -1)
          n-read
          (recur (.read rdr cbuf) (+ n-read data))))))


  (defn read-all-iter
    [^Reader rdr]
    (let [iter (reader->char-buf-iter rdr)]
      (loop [continue? (.hasNext iter)
             n-read 0]
        (if continue?
          (let [^chars buf (.next iter)]
            (recur (.hasNext iter) (+ n-read (alength buf))))
          n-read))))

  (defn read-all-creader
    [^Reader rdr]
    (let [crdr (reader->char-reader rdr)]
      (loop [data (.read crdr)
             n-read 0]
        (if (== data -1)
          n-read
          (recur (.read crdr) (unchecked-inc n-read))))))


  (crit/quick-bench (read-all-reader (io/reader srcpath)))
  ;;27ms
  (crit/quick-bench (read-all-reader (java.io.PushbackReader. (io/reader srcpath))))
  ;;37ms
  (crit/quick-bench (read-all-cbuf (io/reader srcpath)))
  ;;7ms
  (crit/quick-bench (read-all-iter (io/reader srcpath)))
  ;;7ms
  (crit/quick-bench (read-all-creader (io/reader srcpath)))
  ;;17ms

  (def stocks-csv "../../tech.all/tech.ml.dataset/test/data/stocks.csv")
  (def row-iter (read-csv (java.io.File. stocks-csv) {:log-level :info}))
  (def rows (vec (iterator-seq (read-csv (java.io.File. "test/data/funky.csv")))))

  (defn iter-row-count
    [^Iterator iter]
    (loop [continue? (.hasNext iter)
           rc 0]
      (if continue?
        (do
          (.next iter)
          (recur (.hasNext iter) (unchecked-inc rc)))
        rc)))

  (crit/quick-bench (iter-row-count (read-csv (java.io.File. srcpath) {:async? false})))
  ;;26ms
  (crit/quick-bench (iter-row-count (read-csv (java.io.File. srcpath))))
  ;;18ms

  (iter-row-count (read-csv (java.io.File. srcpath) {:log-level :info}))

  )
