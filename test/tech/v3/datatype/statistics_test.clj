(ns tech.v3.datatype.statistics-test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is testing]]
            [clojure.data :as cdata]
            [clojure.pprint :as pp]
            [clojure.edn :as edn]))


(deftest descriptive-statistics
  (let [test-data (double-array (range 100))
        _ (aset test-data 50 Double/NaN)
        stats-ary [:min :max :median :standard-deviation
                   :skew :quartile-1 :quartile-3]]
    (is (dfn/equals
         [0.0 99.0 49.0 29.159 5.3027E-4 24.0 75.0]
         (mapv (stats/descriptive-statistics stats-ary test-data)
               stats-ary))
        (with-out-str
          (pp/pprint
           (cdata/diff
            (stats/descriptive-statistics stats-ary test-data)
            (zipmap stats-ary [0.0 99.0 49.0 29.159 5.3027E-4 24.0 75.0])))))
    (is (dfn/equals [850.252]
                    [(stats/variance test-data)]))))


(deftest percentiles
  (let [test-data (range 50)]
    (is (dfn/equals
         [0.0 11.75 24.5 37.25 49.0]
         (stats/quartiles test-data)))
    (let [test-fn (stats/quartile-outlier-fn test-data)]
      (is (= [true false false true]
             (mapv test-fn [-100 15 50 100])))))
  (testing "an :estimation-type option can be specified"
    (let [percentiles [25 50 75]
          data [1 2 3 4 10]]
      (is (= [1.5 3.0 7.0]
             (stats/percentiles percentiles data)))
      (is (= [1.5 3.0 7.0]
             (stats/percentiles percentiles {:estimation-type :legacy} data)))
      (is (= [2.0 3.0 4.0]
             (stats/percentiles percentiles {:estimation-type :r1} data))))))


(deftest nan-min-max
  (let [test-data (double-array (edn/read-string (slurp "test/data/double-data.edn")))
        {dmin :min dmax :max :as data} (stats/descriptive-statistics [:min :max] test-data)]
    (is (not (Double/isNaN dmin)))
    (is (not (Double/isNaN dmax)))))


(deftest really-do-mode
  (is (= "hey" (-> (stats/descriptive-statistics [:mode] ["hey" "you" "guys" "hey" "hey"])
                   (:mode))))
  (is (nil? (-> (stats/descriptive-statistics [:mode] nil)
                (:mode)))))
