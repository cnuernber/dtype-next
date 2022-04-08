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
    A java.io.Reader-like class that only correctly implements single-character unread but allows access
    to the underlying buffer.
  * [CSVReader.java](https://github.com/cnuernber/dtype-next/blob/master/java/tech/v3/datatype/CSVReader.java) -
    All the tight loops necessary to parse a CSV file quickly.
  * [JSONReader.java](https://github.com/cnuernber/dtype-next/blob/master/java/tech/v3/datatype/JSONReader.java) -
    All the tight loops necessary to parse a JSON file.  Fairly customizeable."
  (:require [clojure.java.io :as io]
            [tech.v3.parallel.queue-iter :as queue-iter]
            [com.github.ztellman.primitive-math :as pmath]
            [clojure.set :as set])
  (:import [tech.v3.datatype CharReader UnaryPredicate UnaryPredicates$LongUnaryPredicate
            CharBuffer IFnDef CSVReader$RowReader JSONReader JSONReader$ObjReader]
           [clojure.lang IFn]
           [java.io Reader StringReader]
           [java.util Iterator Arrays ArrayList List NoSuchElementException]
           [java.util.function Function BiFunction Supplier]
           [java.lang AutoCloseable]
           [org.roaringbitmap RoaringBitmap]))

(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defn- ->reader
  ^Reader [item]
  (cond
    (instance? Reader item)
    item
    (string? item)
    (StringReader. item)
    :else
    (io/reader item)))



(deftype CharBufFn [^{:unsynchronized-mutable true
                      :tag Reader} reader
                    ^{:unsynchronized-mutable true
                      :tag long} cur-buf-idx
                    ^objects buffers
                    close-reader?]
  IFnDef
  (invoke [this]
    (when reader
      (let [^chars next-buf (aget buffers (rem cur-buf-idx
                                               (alength buffers)))
            nchars (.read reader next-buf)]
        (set! cur-buf-idx (unchecked-inc cur-buf-idx))
        (cond
          (pmath/== nchars (alength next-buf))
          next-buf
          (pmath/== nchars -1)
          (do
            (.close this)
            nil)
          :else
          (Arrays/copyOf next-buf nchars)))))
  AutoCloseable
  (close [this]
    (when (and reader close-reader?)
      (.close reader))
    (set! reader nil)))


(defn reader->char-buf-fn
  "Given a reader, return a clojure fn that when called reads the next buffer of the reader.
  This function iterates through a fixed number of buffers under the covers so you need to
  be cognizant of the number of actual buffers that you want to have present in memory.
  This fn also implement `AutoCloseable` and closing it will close the underlying reader.

  Options:

  * `:n-buffers` - Number of buffers to use.  Defaults to 8 - if this number is too small
  then buffers in flight will get overwritten.
  * `:bufsize` - Size of each buffer - defaults to 2048.  Small improvements are sometimes
  seen with larger or smaller buffers.
  * `:async?` - defaults to false.  When true data is read in an async thread.
  * `:close-reader?` - When true, close input reader when finished.  Defaults to true."
  [rdr & [options]]
  (let [rdr (->reader rdr)
        async? (get options :async? false)
        options (if async?
                  ;;You need some number more buffers than queue depth else buffers will be
                  ;;overwritten during processing.  I calculate you need at least 2  - one
                  ;;in the source thread, and one that the system is parsing.
                  (let [qd (long (get options :queue-depth 4))]
                    (assoc options :queue-depth qd
                           :async? true
                           :n-buffers (get options :n-buffers (+ qd 2))
                           :bufsize (get options :bufsize (* 128 1024))))
                  (assoc options :async? false))
        n-buffers (long (get options :n-buffers 2))
        bufsize (long (get options :bufsize (* 128 1024)))
        buffers (object-array (repeatedly n-buffers #(char-array bufsize)))
        src-fn (CharBufFn.  rdr 0 buffers (get options :close-reader? true))]
    (if async?
      (queue-iter/queue-fn src-fn options)
      src-fn)))


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


(defonce char-ary-cls (type (char-array 0)))


(defn reader->char-reader
  "Given a reader, return a CharReader which presents some of the same interface
  as a pushbackreader but is only capable of pushing back 1 character.

  Options:

  Options are passed through mainly unchanged to queue-iter and to
  [[reader->char-buf-iter]].

  * `:async?` - default to true - reads the reader in an offline thread into character
     buffers."
  (^CharReader [rdr options]
   (cond
     (string? rdr)
     (CharReader. ^String rdr)
     (instance? char-ary-cls rdr)
     (CharReader. ^chars rdr)
     :else
     (let [async? (and (> (.availableProcessors (Runtime/getRuntime)) 1)
                       (get options :async? true))
           options (if async? (assoc options :async? true) options)]
       (CharReader. ^IFn (reader->char-buf-fn rdr options)))))
  (^CharReader [rdr]
   (cond
     (string? rdr)
     (CharReader. (str rdr))
     (instance? char-ary-cls rdr)
     (CharReader. ^chars rdr)
     :else
     (CharReader. ^IFn (reader->char-buf-fn rdr nil)))))


(def ^{:private true
       :tag UnaryPredicate}
  true-unary-predicate
  (reify UnaryPredicates$LongUnaryPredicate
    (unaryLong [this arg]
      true)))


(deftype ^:private CSVReadIter [^{:unsynchronized-mutable true
                                  :tag CSVReader$RowReader} rdr
                                close-fn*]
  Iterator
  (hasNext [this] (not (nil? rdr)))
  (next [this]
    (if rdr
      (let [retval (.clone (.currentRow rdr))
            next-row (.nextRow rdr)]
        (when-not next-row
          (.close this))
        retval)
      (throw (NoSuchElementException.))))
  AutoCloseable
  (close [this]
    (set! rdr nil)
    @close-fn*))


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
     speeds up reading larger files (1MB+) by about 30%.
  * `:separator` - Field separator - defaults to \\,.
  * `:quote` - Quote specifier - defaults to //\".
  * `:close-reader?` - Close the reader when iteration is finished - defaults to true.
  * `:column-whitelist` - Sequence of allowed column names.
  * `:column-blacklist` - Sequence of dis-allowed column names.  When conflicts with
     `:column-whitelist` then `:column-whitelist` wins.
  * `:trim-leading-whitespace?` - When true, leading spaces and tabs are ignored.  Defaults to true.
  * `:trim-trailing-whitespace?` - When true, trailing spaces and tabs are ignored.  Defaults
     to true
  * `:nil-empty-values?` - When true, empty strings are elided entirely and returned as nil
     values. Defaults to true."
  ^Iterator [input & [options]]
  (let [rdr (reader->char-reader input options)
        sb (CharBuffer. (get options :trim-leading-whitespace? true)
                        (get options :trim-trailing-whitespace? true)
                        (get options :nil-empty-values? true))
        nil-empty? (get options :nil-empty-values? true)
        quote (->character (get options :quote \"))
        separator (->character (get options :separator \,))
        row-reader (CSVReader$RowReader. rdr sb true-unary-predicate quote separator)
        ;;mutably changes row in place
        next-row (.nextRow row-reader)
        ^RoaringBitmap column-whitelist
        (when (or (contains? options :column-whitelist)
                  (contains? options :column-blacklist))
          (let [whitelist (when-let [data (get options :column-whitelist)]
                            (set data))
                blacklist (when-let [data (get options :column-blacklist)]
                            (set/difference (set data) (or whitelist #{})))
                indexes
                (->> next-row
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
                                   true-unary-predicate)
        close-fn* (delay
                    (when (get options :close-reader? true)
                      (.close rdr)))]
    (when (and next-row column-whitelist)
      (let [^List cur-row next-row
            nr (.size cur-row)
            dnr (dec nr)]
        (dotimes [idx nr]
          (let [cur-idx (- dnr idx)]
            (when-not (.contains column-whitelist cur-idx)
              (.remove cur-row (unchecked-int cur-idx)))))))
    (.setPredicate row-reader col-pred)
    (if next-row
      (CSVReadIter. row-reader close-fn*)
      (do
        @close-fn*
        (reify Iterator
          (hasNext [this] false)
          (next [this] (throw (NoSuchElementException.))))))))


(defn ^:no-doc read-csv-inplace
  "Read a csv returning a function that reads each row and returns the same
  arraylist.  This is used to ensure the overhead of the read-csv pathway is not reasonably
  different than a potentially surprising but maximally efficient inplace pathway."
  [input & [options]]
  (let [rdr (reader->char-reader input options)
        sb (CharBuffer. (get options :trim-leading-whitespace? true)
                        (get options :trim-trailing-whitespace? true)
                        (get options :nil-empty-values? true))
        quote (->character (get options :quote \"))
        separator (->character (get options :separator \,))
        nil-empty? (get options :nil-empty-values? true)
        rowreader (CSVReader$RowReader. rdr sb true-unary-predicate
                                        quote separator)]
    (fn []
      (.nextRow rowreader))))


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


(deftype ^:private JSONReadFn [^{:unsynchronized-mutable true
                                 :tag JSONReader} rdr
                               close-fn*]
  IFnDef
  (invoke [this]
    (when rdr
      (let [nextobj (.readObject rdr)]
        (when-not nextobj
          (.close this))
        nextobj)))
  AutoCloseable
  (close [this]
    (set! rdr nil)
    @close-fn*))

(defn- ->function
  ^Function [data]
  (cond
    (nil? data) data
    (instance? Function data) data
    (instance? IFn data)
    (reify Function (apply [this a] (data a)))
    :else
    (throw (Exception. (format "Unable to convert type %s to java function"
                               (type data))))))

(defn- ->bi-function
  ^BiFunction [data]
  (cond
    (nil? data) data
    (instance? BiFunction data) data
    (instance? IFn data)
    (reify BiFunction (apply [this a b] (data a b)))
    :else
    (throw (Exception. (format "Unable to convert type %s to java bifunction"
                               (type data))))))


(defn- ->supplier
  ^Supplier [data]
  (cond
    (nil? data) data
    (instance? Supplier data) data
    (instance? IFn data)
    (reify Supplier (get [this] (data)))
    :else
    (throw (Exception. (format "Unable to convert type %s to java supplier"
                               (type data))))))


(deftype JSONObj [^objects data])


(defn json-reader-fn
  [options]
  (let [eof-error? (get options :eof-error? true)
        eof-value (get options :eof-value :eof)
        [key-fn val-fn] [(get options :key-fn) (get options :value-fn)]
        [obj-iface-default array-iface-default]
        (case (get options :profile :immutable)
          :mutable
          [JSONReader/mutableObjReader JSONReader/mutableArrayReader]
          :raw
          [JSONReader/mutableObjReader JSONReader/mutableArrayReader]
          :mixed
          [JSONReader/mutableObjReader JSONReader/mutableArrayReader]
          :immutable
          (if (or key-fn val-fn)
            (let [key-fn (or key-fn identity)
                  val-fn (or val-fn (fn val-identity [k v] v))]
              [(reify JSONReader$ObjReader
                 (newObj [this] (transient {}))
                 (onKV [this obj k v]
                   (let [k (key-fn k)
                         v (val-fn k v)]
                     (if-not (identical? v ::elided)
                       (assoc! obj k v)
                       obj)))
                 (finalizeObj [this obj]
                   (persistent! obj)))
               JSONReader/immutableArrayReader])
            [nil nil]))
        obj-iface (get options :obj-iface obj-iface-default)
        array-iface (get options :array-iface array-iface-default)
        bigdec-fn (->function (if (get options :bigdec)
                                #(BigDecimal. ^String %)
                                (get options :double-fn)))
        eof-fn (->supplier
                (get options :eof-fn
                     #(if eof-error?
                        (throw (java.io.EOFException. "Unexpected end of input"))
                        eof-value)))]
    #(JSONReader. bigdec-fn
                  array-iface
                  obj-iface
                  eof-fn)))


(defn read-json-fn
  "Read one or more JSON objects.
  Returns an auto-closeable function that when called by default throws an exception
  if the read pathway is finished.  Input may be a character array or string (most efficient)
  or something convertible to a reader.  Options for conversion to reader are described in
  [[reader->char-reader]] although for the json case we default `:async?` to false as
  most json is just too small to benefit from async reading of the input.

  Options:

  * `:bigdec` - When true use bigdecimals for floating point numbers.  Defaults to false.
  * `:double-fn` - If :bigdec isn't provided, use this function to parse double values.
  * `:profile` - Which performance profile to use.  This simply provides defaults to
     `:array-iface` and `:obj-iface`. The default `:immutable` value produces persistent datastructures and supports value-fn and key-fn.
     `:mutable` produces an object arrays and java.util.HashMaps - this is about
     30% faster.  `:mixed` produces a mixture of persistent maps has hashmaps and is nearly as fast
     as `:raw` while still being very useable and `:raw` produces an object[] for arrays and a
     JSONObj type with a public data member that is an object[] for objects.
  * `:key-fn` - Function called on each string map key.
  * `:value-fn` - Function called on each map value.  Function is passed the key and val so it
     takes 2 arguments.  If this function returns `:tech.v3.datatype.char-input/elided` then
     the key-val pair will be elided from the result.
  * `:array-iface` - Implementation of JSONReader$ArrayReader called on the object array of values for a javascript array.
  * `:obj-iface` - Implementation of JSONReader$ObjReader called for each javascript
    object.  Note that providing this overrides key-fn and value-fn.
  * `:eof-error?` - Defaults to true - when eof is encountered when attempting to read an
     object throw an EOF error.  Else returns a special EOF value.
  * `:eof-value` - EOF value.  Defaults to
  * `:eof-fn` - Function called if readObject is going to return EOF.  Defaults to throwing an
     EOFException."
  [input & [options]]
  (let [^JSONReader json-rdr ((json-reader-fn options))
        close-fn* (delay (.close json-rdr))
        ;;default async? to false
        options (assoc options :async? (get options :async? false))]
    (.beginParse json-rdr (reader->char-reader input options))
    (JSONReadFn. json-rdr close-fn*)))


(defn- read-json-fn-seq
  [json-fn]
  (let [retval (json-fn)]
    (when retval
      (cons retval (lazy-seq (read-json-fn-seq json-fn))))))


(defn read-json
  "Drop in replacement for clojure.data.json/read and clojure.data.json/read-str.  For options
  see [[read-json-fn]]."
  [input & args]
  (let [json-fn (read-json-fn input (into {} (map vec (partition 2 args))))
        retval (json-fn)]
    (.close ^AutoCloseable json-fn)
    retval))


(defn parse-json-fn
  "Return a function from input->json.  Reuses the parse context and thus when
  parsing many small JSON inputs where you intend to get one and only one JSON
  object from them this pathway is a bit more efficient than read-json.

  Same options as [[read-json-fn]]."
  [& [options]]
  (let [json-rdr-fn (json-reader-fn options)
        ;;default async to false
        options (assoc options :async? (get options :async? false))]
    (fn [input]
      (let [^JSONReader json-rdr (json-rdr-fn)]
        (with-open [rdr (reader->char-reader input options)]
          (.beginParse json-rdr rdr)
          (.readObject json-rdr))))))


(comment
  (do
    (require '[clojure.java.io :as io])
    (require '[criterium.core :as crit])
    (def srcpath "../../tech.all/tech.ml.dataset/test/data/issue-292.csv"))


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
  (crit/quick-bench (iter-row-count (read-csv (java.io.File. srcpath) {:bufsize 8192})))
  ;;16.6ms

  (defn inplace-row-count
    ^long [read-fn]
    (loop [row (read-fn)
           rc 0]
      (if row
        (recur (read-fn) (unchecked-inc rc))
        rc)))

  (crit/quick-bench (inplace-row-count (read-csv-inplace (java.io.File. srcpath))))

  (crit/quick-bench (inplace-row-count (read-csv-inplace (java.io.File. srcpath)
                                                         {:bufsize 8192})))

  (do
    (require '[tech.v3.datatype :as dtype])
    (require '[tech.v3.datatype.functional :as dfn])
    (defn ^:no-doc timeop-ns
      ^long [op]
      (let [start (System/nanoTime)
            _ (op)
            end (System/nanoTime)]
        (- end start)))

    (defn ^:no-doc avg-time-ms
      [op]
      ;;warmup
      (op)
      (* (dfn/mean (repeatedly 5 #(timeop-ns op))) 1e-6)))

  (avg-time-ms #(inplace-row-count (read-csv-inplace (java.io.File. srcpath))))

  )
