(ns tech.v3.datatype-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.nio-buffer :as nio-buffer]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.parallel.for :as parallel-for]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.unary-pred :as unary-pred]
            [tech.v3.datatype.rolling :as rolling]
            [tech.v3.datatype.gradient :as dt-grad]
            [tech.v3.datatype.wavelet]
            [tech.v3.datatype.datetime]
            [ham-fisted.lazy-noncaching :as lznc]
            [ham-fisted.api :as hamf]
            [ham-fisted.protocols :as hamf-proto]
            [benchmark.api :as bench])
  (:import [java.nio FloatBuffer]
           [java.util ArrayList]
           [ham_fisted Casts]
           [tech.v3.datatype FastStruct]))


(defn basic-copy
  [src-ctype dest-ctype src-dtype dst-dtype]
  (try
    (let [ary (dtype/make-container src-ctype src-dtype (range 10))
          buf (dtype/make-container dest-ctype dst-dtype 10)
          retval (dtype/make-container :float64 10)]
      ;;copy starting at position 2 of ary into position 4 of buf 4 elements
      (dtype/copy! (dtype/sub-buffer ary 2 4) (dtype/sub-buffer buf 4 4))
      (dtype/copy! buf retval)
      (is (= [0 0 0 0 2 3 4 5 0 0]
             (mapv int (dtype/->vector retval)))
          (str [src-ctype dest-ctype src-dtype dst-dtype]))
      (is (= 10 (dtype/ecount buf))))
    (catch Throwable e
      (throw (ex-info (str src-ctype "<" src-dtype ">, " dest-ctype "<" dst-dtype ">")
                      {:error e}))
      (throw e))))


(def create-functions [:jvm-heap :native-heap-LE :native-heap-BE :list])


(deftest generalized-copy-test
  (->> (for [src-container create-functions
             dst-container create-functions
             src-dtype casting/numeric-types
             dst-dtype casting/numeric-types]
         (basic-copy src-container dst-container src-dtype dst-dtype))
       dorun))


(deftest raw-copy-with-mutable-lazy-sequence
  ;;It is important that raw copy can work with a lazy sequence of double buffers where
  ;;the same buffer is being filled for ever member of the lazy sequence.  This is an
  ;;easy optimization to make that cuts down the memory usage when reading from datasets
  ;;by a fairly large amount.
  (let [input-seq (partition 10 (range 100))
        input-ary (double-array 10)
        ;;Note the input ary is being reused.  Now imagine the input ary is a large
        ;;and we want a batch size of 50.  This is precisely the use case
        ;;where we don't want to allocate dynamically an entire batch worth of data
        ;;every time but copy one image at a time into the packed buffer for upload
        ;;to the gpu.
        double-array-seq (lznc/map (fn [data]
                                     (dtype/copy-raw->item! data input-ary 0)
                                     input-ary)
                                   input-seq)
        output-doubles (double-array 100)]
    (dtype/copy-raw->item! double-array-seq output-doubles 0)
    (is (= (vec output-doubles) (mapv double (flatten input-seq))))))


(deftest array-of-array-support
  (let [src-data (into-array (repeat 10 (double-array (range 10))))
        dst-data (float-array 100)]
    ;;This should not hit any slow paths.
    (dtype/copy-raw->item! src-data dst-data 0)
    (is (= (vec (apply concat src-data))
           (vec dst-data)))))


(deftest out-of-range-data-causes-exception
  (is (thrown? Throwable (dtype/copy! (int-array [1000 2000 3000 4000])
                                      (byte-array 4)))))


(deftest offset-buffers-should-copy-correctly
  (let [fbuf (-> (dtype/make-container :float32 (range 10))
                 (dtype/sub-buffer 3))
        result-buf (dtype/make-container :float32 (dtype/ecount fbuf))]
    (dtype/copy! fbuf result-buf)
    (is (= (dtype/get-value fbuf 0)
           (float 3)))
    (is (= (vec (drop 3 (range 10)))
           (mapv long (dtype/->vector result-buf))))))


(deftest primitive-types-are-typed
  (doseq [[cls dtype] [[(byte 1) :int8]
                       [(short 1) :int16]
                       [(char 1) :char]
                       [(int 1) :int32]
                       [(long 1) :int64]
                       [(float 1) :float32]
                       [(double 1) :float64]
                       [(boolean false) :boolean]]]
    (is (= dtype (dtype/elemwise-datatype cls)))))


(deftest copy-time-test
  (testing "Run perf regression of times spent to copy data"
    (let [num-items (long 100000)
          src-data (float-array (range num-items))
          dst-data (float-array num-items)
          array-copy (fn []
                       (parallel-for/parallel-for
                        idx num-items
                        (aset dst-data idx (aget src-data idx))))
          src-buf (FloatBuffer/wrap src-data)
          dst-buf (FloatBuffer/wrap dst-data)
          buffer-copy (fn []
                        (let [
                              ;; Curiously, uncommenting this gets a far faster result.
                              ;; But it isn't at all practical.
                              ;; src-buf (FloatBuffer/wrap src-data)
                              ;; dst-buf (FloatBuffer/wrap dst-data)
                              ]
                          (parallel-for/parallel-for
                           idx num-items
                           (.put dst-buf idx (.get src-buf idx)))))
          src-nbuf (dtype/make-container :native-heap :float32 (range num-items))
          dst-nbuf (dtype/make-container :native-heap :float32 num-items)

          dtype-copy (fn []
                       (dtype/copy! src-nbuf dst-nbuf num-items))

          dtype-clone-ary (fn [] (dtype/clone src-data))

          make-array (fn []
                       (dtype/make-container :java-array :float32 dst-buf))
          marshal-buf (int-array num-items)
          ;;If you have to do a marshalling copy then exploiting parallelism will be
          ;;your best bet.  It costs a lot to marshal across datatypes, esp. int->float.
          marshal-copy (fn []
                         (dtype/copy! src-data marshal-buf))
          fns {:array-copy array-copy
               :buffer-copy buffer-copy
               :dtype-copy dtype-copy
               :make-array make-array
               :dtype-clone-ary dtype-clone-ary
               :marshal-copy marshal-copy}
          run-timed-fns (fn []
                          (->> fns
                               (map (fn [[fn-name time-fn]]
                                      [fn-name (with-out-str
                                                 (time
                                                  (dotimes [iter 400]
                                                    (time-fn))))]))
                               (into {})))
          warmup (run-timed-fns)
          times (run-timed-fns)]
      (println times))))



#_(deftest nil-has-nil-shape
    (is (= nil (dtype/shape nil)))
    (is (= 0 (dtype/ecount nil)))
    (is (= 0 (dtype/shape->ecount (dtype/shape nil)))))


(deftest boolean-support
  (is (= [0 1 0]
         (-> (dtype/make-container :java-array :int8 [false true false])
             (dtype/->vector))))


  (is (= [0 1 0]
         (-> (dtype/make-container :java-array :int8
                                   (boolean-array [false true false]))
             (dtype/->vector))))


  (is (= [0 1 0]
         (-> (dtype/copy! (boolean-array [false true false])
                          (dtype/make-container :int8 3))
             (dtype/->vector))))


  (is (= [false true false]
         (-> (dtype/make-container :boolean [0 1 0])
             (dtype/->vector))))


  (is (= [false true false]
         (-> (dtype/make-container :boolean [0.0 0.01 0.0])
             (dtype/->vector))))

  (is (= true (dtype/cast 10 :boolean)))

  (is (= false (dtype/cast 0 :boolean)))

  (is (= 1 (dtype/cast true :int16)))

  (is (= 0.0 (dtype/cast false :float64))))


(deftest list-regression
  (is (= (Class/forName "[D")
         (type (dtype/->double-array
                (dtype/make-container :list :float64 10)))))
  (is (= (Class/forName "[D")
         (type (dtype/->double-array
                (-> (dtype/make-container :list :float64 10)
                    (dtype/sub-buffer 2 2)))))))


#_(deftest nested-array-things-have-appropriate-shape
  (is (= [5 5]
         (->> (repeatedly 5 #(double-array 5))
              (into-array)
              dtype/shape)))
  (is (= 25
         (->> (repeatedly 5 #(double-array 5))
              (into-array)
              dtype/ecount)))
  (is (= [5 5]
         (->> (repeatedly 5 #(range 5))
              (into-array)
              dtype/shape)))
  (is (= 25
         (->> (repeatedly 5 #(range 5))
              (into-array)
              dtype/ecount))))


(deftest base-math-sanity
  (is (= 0.0 (-> (dfn/- (range 10) (range 10))
                 (dfn/pow 2)
                 (dfn/reduce-+))))
  (is (= 0.0 (-> (dfn/- (into-array (range 10)) (range 10))
                 (dfn/pow 2)
                 (dfn/reduce-+)))))


(deftest nan-insanity
  (let [test-floats (float-array [0 ##NaN 2 4 ##NaN])
        nan-floats (float-array (repeat 5 ##NaN))]
    (is (= [1 4]
           (-> (argops/argfilter :tech.numerics/nan? test-floats)
               vec)))
    (is (= [false true false false true]
           (dfn/nan? test-floats)))

    (is (= [false true false false true]
           (dfn/eq test-floats Float/NaN)))

    (is (= [1 4]
           (->> (dfn/eq test-floats nan-floats)
                (argops/argfilter identity)
                vec)))))


(deftest round-and-friends
  (is (= [2 3 4 5]
         (vec (dfn/round [2.2 2.8 3.5 4.6]))))
  (is (= [2.0 2.0 3.0 4.0]
         (vec (dfn/floor [2.2 2.8 3.5 4.6]))))
  (is (= [3.0 3.0 4.0 5.0]
         (vec (dfn/ceil [2.2 2.8 3.5 4.6]))))
  (is (dfn/equals [2.2 2.8 3.5 4.6]
                  (vec (dfn/abs [2.2 2.8 3.5 4.6]))))
  (is (= [1 4 9 16]
         (vec (dfn/sq (byte-array [1 2 3 4])))))
  (is (dfn/equals [0.381 0.540 0.675 0.785]
                  (dfn/atan2 [2 3 4 5] 5)
                  0.001))
  (is (thrown? Throwable (dfn/rem (double-array [1 2 3 4] 2))))
  (is (= [1 0 1 0]
         (dfn/rem [1 2 3 4] 2))))


(deftest generic-lists
  (is (= (vec (range 200 255))
         (-> (dtype/make-container :list :uint8 (range 200 255))
             dtype/->vector))))


(deftest desc-stats-unsigned-types
  (is (= {:min 0.0
          :max 255.0}
         (-> (dtype/make-container :typed-buffer :uint8 (range 256))
             (dfn/descriptive-statistics)
             (select-keys [:min :max])))))


(deftest reader-sorting
  (doseq [datatype (->> casting/base-host-datatypes
                        (remove #(= :boolean %)))]
    (let [breader (dtype/->reader (dtype/make-container :java-array datatype
                                                        (reverse (range 10))))]
      (is (= (vec (range 10))
             (vec (map int (sort breader))))))))


(deftest object->string-reader-cast
  (is (= :string
         (-> (dtype/elemwise-cast ["a" "b"] :string)
             (dtype/get-datatype)))))


(deftest object->uint16
  (is (= :uint16
         (-> (dtype/elemwise-cast [1 2] :uint16)
             (dtype/get-datatype)))))


(defn copy-raw->item-time-test-1
  []
  (let [n-elems 10000
        dst-ary (float-array n-elems)
        src-data (map float (range n-elems))]
    (time (dotimes [iter 1000]
            (dtype/copy-raw->item! src-data dst-ary)))))


(defn copy-raw->item-time-test-2
  []
  (let [n-elems 10000
        n-seq-items 100
        dst-ary (float-array (* n-elems n-seq-items))
        src-data (map float-array (repeat n-seq-items (range n-elems)))]
    (time (dotimes [iter 100]
            (dtype/copy-raw->item! src-data dst-ary)))))


(deftest arggroup-by-test
  ;;The operation returns values in object space
  ;;if it operates as :object datatype.  This is the default.
  (let [{truevals true
         falsevals false}
        (argops/arggroup-by even? (range 20))]
    (is (= (set truevals)
           (set (filter even? (range 20)))))
    (is (= (set falsevals)
           (set (remove even? (range 20))))))


  ;;Since the arguments is a long reader, the operation
  ;;operates and returns values in long space.
  (let [{truevals true
         falsevals false}
        (argops/arggroup-by even? {:storage-datatype :bitmap} (range 20))]
    (is (= (set truevals)
           (set (filter even? (range 20)))))
    (is (= (set falsevals)
           (set (remove even? (range 20)))))))


(deftest arggroup-by-int-test
  ;;The operation returns values in object space
  ;;if it operates as :object datatype.  This is the default.
  (let [{truevals true
         falsevals false}
        (argops/arggroup-by even? {:storage-datatype :int32} (range 20))]
    ;;Default group by is ordered
    (is (= truevals
           (filter even? (range 20))))
    (is (= falsevals
           (remove even? (range 20)))))


  (let [{truevals true
         falsevals false}
        (argops/arggroup-by even? {:storage-datatype :int64} (range 20))]
    (is (= truevals
           (filter even? (range 20))))
    (is (= falsevals
           (remove even? (range 20))))))


(deftest arggroup-by-bitmap-test
  ;;The operation returns values in object space
  ;;if it operates as :object datatype.  This is the default.
  (let [{truevals true
         falsevals false}
        (argops/arggroup-by even? {:storage-datatype :bitmap} (range 20))]
    (is (= (vec truevals)
           (vec (filter even? (range 20)))))
    (is (= (vec falsevals)
           (vec (remove even? (range 20)))))))


(deftest argpartition-by-test
  ;;The operation returns values in object space
  ;;if it operates as :object datatype.  This is the default.
  (is (= [[0 (range 5)]
          [1 (range 5 10)]
          [2 (range 10 15)]
          [3 (range 15 20)]]
         (vec (argops/argpartition-by #(quot (long %) 5) (range 20))))))


(deftest typed-buffer-destructure
  (let [[a b c] (dtype/make-container :jvm-heap :int64 [1 2 3])]
    (is (= [a b c]
           [1 2 3]))))


(deftest simple-mse
  (let [predictions (list 1 2 3 4 5)
        labels (list 3 4 5 6 7)]
    (is (= 20.0
           (-> (dfn/- predictions labels)
               (dfn/pow 2)
               (dfn/reduce-+))))))


(deftest float-long->double
  (is (= :float64
         (dtype/get-datatype
          (dfn/+ (float-array 5) (long-array 5)))))
  (is (= :float64
         (dtype/get-datatype
          (dfn/+ (long-array 5) (float-array 5))))))


(deftest primitives-arent-readers
  (is (not (dtype/reader? 2))))


(deftest infinite-ranges-arent-readers
  (is (not (dtype/reader? (range))))
  (is (dtype/reader? (range 5))))


(deftest nil-isnt-a-reader
  (is (not (dtype/reader? nil))))


(deftest reduce-+-iterable
  (is (= (apply + (range 1000))
         (-> (range 1000) dfn/reduce-+))))


(deftest array-list-writers
  (is (not (nil? (dtype/->writer (ArrayList.)))))
  (let [new-list (ArrayList. ^java.util.Collection (repeat 10 nil))
        writer (dtype/->writer new-list)]
    (.writeFloat writer 0 10)
    (.writeFloat writer 5 99)
    (is (= [10.0 nil nil nil nil 99.0 nil nil nil nil]
           (vec new-list)))))


(deftest clone-for-ranges-persistent-vectors
  (is (= [1 2 3] (dtype/clone [1 2 3])))
  (is (= (vec (range 10))
         (vec (dtype/clone (range 10))))))


(deftest persistent-vectors-are-not-writers
  (is (not (dtype/writer? [1 2 3]))))


(deftest is-mathematical-integer?
  (is (= [true false true true false]
         (vec (dfn/mathematical-integer? [1.0 1.1 1000 1.26e4 1.26e-3])))))


(deftest argsort-generic
  (let [data (dtype/make-container :java-array :int16 (shuffle
                                                       (range 10)))
        indexes (argops/argsort :tech.numerics/> data)
        new-data (dtype/indexed-buffer indexes data)]
    (is (= (vec (reverse (range 10)))
           (vec new-data)))))


(deftest argsort-nan
  (let [data (dtype/make-container :float64 [##NaN 2 5 ##NaN 1 6 3 4 ##NaN])
        idx-first (argops/argsort nil {:nan-strategy :first} data)
        idx-last (argops/argsort nil {:nan-strategy :last} data)]
    (is (= [0 3 8] (vec (take 3 idx-first))))
    (is (= [0 3 8] (vec (take-last 3 idx-last))))
    (is (thrown? Exception (argops/argsort nil {:nan-strategy :exception} data)))))


(deftest reader-as-persistent-vector-test
  (let [src-data (range 20)
        ldata (long-array src-data)
        ;;Noncopying,in-place conversion
        buffers (->> (range 4)
                     (map #(dtype/sub-buffer ldata (* (long %) 5) 5)))
        ;;copying conversion
        vectors (map (comp vec dtype/->reader) buffers)
        map-fn (fn [item-seq]
                 (->> item-seq
                      (map-indexed (fn [idx item]
                                     [item idx]))
                      (into {})))]
    (is (= (map-fn buffers)
           (map-fn vectors)))))


(deftest all-readers-are-sequential
  (is (every? sequential? (->> (casting/all-datatypes)
                               (map #(-> (dtype/make-container :java-array % 5)
                                         (dtype/->reader)))))))


(deftest make-readers
  (is (= [0.0 1.0]
         (dtype/make-reader :float64 2 idx)))
  ;;Ensure that you can correctly alias datatypes
  (is (= :packed-local-date
         (dtype/get-datatype
          (dtype/make-reader :packed-local-date 2 idx)))))


(deftest object-array-mean
  (is (= 4.5 (dfn/mean (object-array (range 10))))))


(deftest float-spaces
  (is (dfn/equals [0.0 0.6931471805599453 1.0986122886681098 1.3862943611198906]
                  (dfn/log (range 1 5))))
  (is (dfn/equals [0.0 1.0 4.0]
                  (dfn/pow (range 3) 2)))
  (is (dfn/equals [3.5 2.5 1.5]
                  (dfn/rem [12.3 11.3 10.3] 4.4))))


(deftest array-clone
  (let [dtypes (concat [:boolean :object :keyword] casting/host-numeric-types)]
    (doseq [dtype dtypes]
      (let [new-container (dtype/clone (dtype/make-container :java-array dtype 5))]
        (is (= dtype (dtype/get-datatype new-container)))))))


(deftest apply-works-with-readers
  (let [dtypes (concat [:boolean :object :keyword] casting/host-numeric-types)]
    (doseq [dtype dtypes]
      (let [new-container (dtype/clone (dtype/make-container :java-array dtype 5))]
        (if-not (or (= dtype :object)
                    (= dtype :keyword))
          (is (not (nil? (apply (dtype/->reader new-container) [1])))
              (format "Failed for datatype %s" dtype))
          (is (nil? (apply (dtype/->reader new-container) [1]))))))))


(deftest clone-works-with-typed-lists
  (is (= [] (vec (dtype/clone (dtype/make-container :list :string 0)))))
  (is (= ["one"] (vec (dtype/clone (dtype/make-container :list :string ["one"]))))))


(deftest fill-range
  (let [test-data [1 3 8 20]
        {:keys [result missing]} (dfn/fill-range test-data 3)
        n-results (dtype/ecount result)]
    (is (= #{ 4 6 2 5} (set missing)))
    (is (= 8 n-results))
    (is (dfn/equals [1.0 3.0 5.5 8.0 11.0 14.0 17.0 20.0]
                    result))

    ;;Now need enough data to trigger parallelism
    (let [offset-range [0 25 50 75 100 125]
          long-test-data (->> offset-range
                              (map #(dfn/+ test-data %))
                              (flatten)
                              vec)
          {:keys [result missing]} (dfn/fill-range long-test-data 3)])))


(deftest argshuffle-test
  (let [test-data (dtype/make-container :float32 (range 20))
        buf (dtype/->buffer test-data)]
    (is (= (vec (argops/argshuffle 20 {:seed 20}))
           (vec (argops/argshuffle 20 {:seed 20}))))
    (is (not= (vec (argops/argshuffle 20))
              (vec (argops/argshuffle 20))))))


(deftest plist-addall
  (let [test-list (dtype/make-list :int64)]
    (java.util.Collections/addAll test-list (into-array (range 10)))
    (is (= (vec (range 10)) (vec test-list)))))


(deftest ctime-min-max
  (is (dtype-proto/has-constant-time-min-max? 4))
  (is (= 4 (dtype-proto/constant-time-min 4)))
  (is (= 4 (dtype-proto/constant-time-max 4)))
  (is (dtype-proto/has-constant-time-min-max? (range 4)))
  (is (= 0 (dtype-proto/constant-time-min (range 4))))
  (is (= 3 (dtype-proto/constant-time-max (range 4))))
  )

(deftest reduce-min-max
  (let [amin (dfn/reduce-min (range 10))
        amax (dfn/reduce-max (range 10))
        prod (dfn/reduce-* (range 1 11))]
    (is (= amin 0))
    (is (= amax 9))
    (is (= prod 3628800))))


(deftest rolling-window-position
  (is (= (mapv double [0 1 3 6 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85])
         (rolling/fixed-rolling-window (range 20) 5 dfn/sum
                                       {:relative-window-position :left})))
  (is (= (mapv double [3 6 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 89 92])
         (rolling/fixed-rolling-window (range 20) 5 dfn/sum
                                       {:relative-window-position :center})))
  (is (= (mapv double [10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 89 92 94 95])
       (rolling/fixed-rolling-window (range 20) 5 dfn/sum
                                     {:relative-window-position :right})))
  (is (= [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19]
         (rolling/fixed-rolling-window (range 20) 5 (fn [w] (last w))
                                       {:relative-window-position :left}))))


(deftest rolling-window-position-seq
  (is (dfn/equals [0 1 3 6 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85]
                  (rolling/fixed-rolling-window (apply list (range 20)) 5 dfn/sum
                                                {:relative-window-position :left})))
  (is (dfn/equals [3 6 10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 89 92]
                  (rolling/fixed-rolling-window (apply list (range 20)) 5 dfn/sum
                                                {:relative-window-position :center})))
  (is (dfn/equals [10 15 20 25 30 35 40 45 50 55 60 65 70 75 80 85 89 92 94 95]
                  (rolling/fixed-rolling-window (apply list (range 20)) 5 dfn/sum
                                                {:relative-window-position :right})))
  (is (dfn/equals [0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19]
                  (rolling/fixed-rolling-window (apply list (range 20)) 5
                                                (fn [w] (last w))
                                                {:relative-window-position :left}))))


(deftest binary-search
  (is (= [0 0 5 9 10]
         (mapv #(argops/binary-search (double-array (range 10)) %)
               [-1 0 4.5 9 10]))))


(deftest binary-search
  (is (= [0 0 4 9 10]
         (mapv #(argops/binary-search (range 10) %)
               [-1 0 4.5 9 10]))))


(deftest binary-search
  (is (= [0 0 4 5 6]
         (mapv #(argops/binary-search (vec "bcdefg") %)
               [\a \b \f \g \h]))))


(deftest seq-empty-stats
  (let [stats #{:min :max :n-elems :median}
        dfn-map-eq (fn [key-seq expected data-map]
                     (every? identity (dfn/eq expected
                                              (mapv data-map key-seq))))]
    (is (= {:min 1, :median 2, :max 4 :n-elems 3}
           (->> (dfn/descriptive-statistics (list 1 2 ##NaN 4) stats)
                (map (fn [[k v]]
                       [k (int v)]))
                (into {}))))
    (is (dfn-map-eq [:min :median :max :n-elems] [##NaN ##NaN ##NaN 0]
                    (dfn/descriptive-statistics (list ##NaN ##NaN ##NaN) stats)))
    (is (dfn-map-eq [:min :max :n-elems] [##NaN ##NaN 0]
                    (dfn/descriptive-statistics
                     (list ##NaN ##NaN ##NaN)
                     (disj stats :median))))
    (is (dfn-map-eq [:mean :standard-deviation]
                    [##NaN 0.0]
                    (dfn/descriptive-statistics (list) #{:mean :standard-deviation})))
    (is (dfn-map-eq [:mean :standard-deviation]
                    [##NaN 0.0]
                    (dfn/descriptive-statistics nil #{:mean :standard-deviation})))))

(deftest tricky-double-values
  (is (dfn/eq 0.0 -0.0))
  (is (dfn/eq Double/NaN Double/NaN)))


(deftest really-ordered-group-by
  (let [groups (argops/arggroup-by #(mod % 13) (range 200))]
    (is (dfn/eq (range 13) (vec (keys groups))))))


(deftest elemwise-cast-vec-of-vec
  (is (= [[2 3]]
         (dtype/elemwise-cast [[2 3]] :persistent-vector))))


(deftest indexed-reader-range-sub-buffer
  (let [ary-data (dtype/make-container :float32 (range 100))
        idx-rdr (dtype/indexed-buffer (range 5 20) ary-data)]
    ;;This conversion relies on a specific optimization that uses sub-buffer
    ;;if the indexes passed to indexed-rdr are a range
    (is (not (nil? (dtype/as-array-buffer idx-rdr))))))


(deftest custom-reader-booleans-26
  (is (= [false true false true false]
         (-> (dtype/make-reader :int32 5 idx)
             (dfn/odd?)
             vec))))


(deftest iterator-pathway-fails-count-29
  (is (= 3 (count (dfn/+ 3 (repeat 3 3))))))


(deftest basic-can-write-queries
  (is (not (dtype/writer? (dtype/->reader [1 2 3] :int32))))
  (is (not (dtype/writer? (dtype/make-reader :int32 4 idx))))
  (is (dtype/writer? (dtype/make-container :int32 4)))
  (is (dtype/writer? (int-array 5))))


(deftest ->reader-bad-dtype
  (is (thrown? Throwable (dtype/->reader [1 2 3] :foo))))


(deftest set-constant-all-numeric-dtypes
  (let [dtypes [:int8 :uint8
                :int16 :uint16
                :int32 :uint32
                :int64 :uint64
                :float32 :float64]
        containers [:jvm-heap :native-heap]]
    (dorun
     (for [dtype dtypes
           container containers]
       (let [data (dtype/make-container container dtype 10)]
         (dtype/set-constant! data 1)
         (is (= (mapv long (dtype/->reader data))
                (vec (repeat 10 1)))))))))


(deftest nth-neg-indexes
  (let [data (dtype/make-container :int64 (range 10))]
    (is (thrown? Throwable (nth data 10)))
    (is (= :a (nth data 10 :a)))
    (is (thrown? Throwable (nth data -11)))
    (is (= :a (nth data -11 :a)))
    (is (= 0 (nth data -10 :a))))
  (let [data (dtype/make-list :int64 (range 10))]
    (is (thrown? Throwable (nth data 10)))
    (is (= :a (nth data 10 :a)))
    (is (thrown? Throwable (nth data -11)))
    (is (= :a (nth data -11 :a)))
    (is (= 0 (nth data -10 :a)))))


(deftest elemwise-cast-container
  (let [c (dtype/make-container :float64 [1.2])
        ic (dtype/elemwise-cast c :int32)]
    (is (= 1 (ic 0))))
  (let [c (dtype/make-container :float64 [155])
        ic (dtype/elemwise-cast c :int8)]
    (is (thrown? Throwable (ic 0))))
  (let [c (dtype/make-container :float64 [155])
        ic (dtype/elemwise-cast c :uint8)]
    (is (= 155 (ic 0))))
  ;;This one is tricky because the JVM is too stupid to cast
  ;;an char, which is an unsigned short, to any numeric type without
  ;;more magic.
  (let [c (dtype/make-container :char [155])
        ic (dtype/elemwise-cast c :uint8)]
    (is (= 155 (ic 0)))))


(deftest issue-46-tri-bool-ops
  (is (= [false false false true true true true false false false]
         (vec (dfn/< 2 (range 10) 7))))
  (is (= [false false true true true true true true false false]
         (vec (dfn/<= 2 (range 10) 7))))
  (is (= [false false true true true true true false false false]
         (vec (dfn/> 7 (range 10) 1))))
  (is (= [false true true true true true true true false false]
         (vec (dfn/>= 7 (range 10) 1)))))


(deftest mean-sum-forgiveness
  (let [test-data [Double/NaN 1 2]]
    (is (not (Double/isNaN (dfn/sum test-data))))
    (is (not (Double/isNaN (dfn/mean test-data))))))


(deftest gradient-correct
  (is (= (vec (repeat 10 1.0))
         (dt-grad/gradient1d (range 10)))))


(deftest double-nan-boolean
  (is (= false (dtype/cast (Double/NaN) :boolean)))
  (is (= [false]
         (-> (dtype/make-container :float64 [Double/NaN])
             (dtype/elemwise-cast :boolean)
             (vec))))
  (is (= false (Casts/booleanCast Double/NaN)))
  (is (= false (Casts/booleanCast (Double/valueOf Double/NaN))))
  (is (= false (Casts/booleanCast (Long/valueOf 0))))
  (is (= [2] (vec (argops/argfilter identity (double-array [0 Double/NaN 1]))))))


(deftest bool-list
  (is (= :boolean (dtype/elemwise-datatype (dtype/make-list :boolean)))))


(deftest unsigned-to-array-to-array-buffer
  (doseq [dtype [:uint64 :uint32 :uint16 :uint8]]
    (let [c (dtype/make-container dtype (range 200))
          abuf (dtype/->array-buffer c)
          data (dtype/->array c)]
      (is (dfn/equals abuf c) dtype)
      (is (dfn/equals data c) dtype))))


(deftest faststruct-test
  (let [slots (hamf/java-hashmap [[:a 0] [:b 1] [:c 2] [:d 3]])
        lhs (FastStruct. slots (hamf/vector 1 2 3 4))
        rhs (FastStruct. slots (hamf/vector 2 3 4 5))
        llhs (FastStruct. slots (hamf/vector 1 2 3 4))]
    (is (not (= lhs rhs)))
    (is (= lhs llhs))
    (is (= {:a 1 :b 2 :c 3 :d 4} lhs))
    (is (= lhs {:a 1 :b 2 :c 3 :d 4}))
    (is (not= lhs (assoc lhs :e 4)))
    (is (= (assoc lhs :e 4) (assoc lhs :e 4)))))


(deftest empty-index-reduce-merge
  (let [ifn (-> (unary-pred/index-reducer :int32)
                (hamf-proto/->init-val-fn))]
    (is (= []
           (-> (.reduce (ifn) (ifn))
               (deref))))))


(deftest arggroup-order
  (is (= [:a :b :c :d :e :f]
         (->> (argops/arggroup
               (into [] cat [[:a :b :c :d :e :f]
                             (shuffle (take 1000 (cycle [:a :b :c :d :e :f])))]))
              (keys)
              (vec)))))


(comment
  (defn bench-sum
    []
    (vec
     (for [n-elems [10 100 1000000]]
       (let [data (hamf/double-array (hamf/range n-elems))]
         {:n-elems n-elems
          :dtype (bench/benchmark-us (dfn/sum data))
          :hamf (bench/benchmark-us (hamf/sum data))
          }))))

  (defn bench-ordered-unordered-sum
    []
    ;;logically unordered should always be faster but there is sometimes
    ;;significant overhead for adding the extra queue in for unordered results.
    ;;This test results in no meaningful difference however.
    (vec
     (for [n-elems [10 100 1000 10000 1000000]]
       (let [data (hamf/double-array (hamf/range n-elems))]
         {:n-elems n-elems
          :ordered (bench/benchmark-us (hamf/sum data {:min-n 0
                                                       :ordered? true}))
          :unordered (bench/benchmark-us (hamf/sum data {:min-n 0
                                                         :ordered? false}))
          }))))

  (defn bench-parallel-serial-sum
    []
    (vec
     (for [n-elems [10 100 1000 10000 1000000]]
       (let [data (hamf/double-array (hamf/range n-elems))]
         {:n-elems n-elems
          :serial (bench/benchmark-us (hamf/sum data {:min-n Integer/MAX_VALUE}))
          :parallel (bench/benchmark-us (hamf/sum data {:min-n 0}))}))))
  )
