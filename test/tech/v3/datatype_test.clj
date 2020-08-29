(ns tech.v3.datatype-test
  (:require [clojure.test :refer [deftest is]]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting]))


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
