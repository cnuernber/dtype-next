(ns tech.v3.datatype-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.parallel.for :as parallel-for])
  (:import [java.nio FloatBuffer]))


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
      (throw (ex-info (str [src-ctype dest-ctype src-dtype dst-dtype])
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
        double-array-seq (map (fn [data]
                                (dtype/copy-raw->item! data input-ary 0)
                                input-ary)
                              input-seq)
        output-doubles (double-array 100)]
    (dtype/copy-raw->item! double-array-seq output-doubles 0)
    (is (= (vec output-doubles) (mapv double (flatten input-seq))))))


(deftest array-of-array-support
  (let [^"[[D" src-data (make-array (Class/forName "[D") 5)
        _ (doseq [idx (range 5)]
            (aset src-data idx (double-array (repeat 10 idx))))
        dst-data (float-array (* 5 10))]
    ;;This should not hit any slow paths.
    (dtype/copy-raw->item! src-data dst-data 0)
    (is (= (vec (float-array (flatten (map #(repeat 10 %) (range 5)))))
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


          make-array (fn []
                       (dtype/make-container :java-array :float32 dst-buf))
          marshal-buf (int-array num-items)
          ;;If you have to do a marshalling copy then exploiting parallelism will be
          ;;your best bet.  It costs a lot to marshal across datatypes, esp. int->float.
          marshal-copy (fn []
                         (dtype/copy! src-data 0
                                      marshal-buf 0
                                      num-items
                                      {:unchecked? true}))
          fns {:array-copy array-copy
               :buffer-copy buffer-copy
               :dtype-copy dtype-copy
               :make-array make-array
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
