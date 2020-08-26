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


(def create-functions [:jvm-heap :native-heap-LE :native-heap-BE])


(deftest generalized-copy-test
  (->> (for [src-container create-functions
             dst-container create-functions
             src-dtype casting/numeric-types
             dst-dtype casting/numeric-types]
         (basic-copy src-container dst-container src-dtype dst-dtype))
       dorun))
