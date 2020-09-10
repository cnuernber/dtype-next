(ns tech.v3.datatype.statistics_test
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.statistics :as stats]
            [tech.v3.datatype.functional :as dfn]
            [clojure.test :refer [deftest is]]))


(deftest descriptive-statistics
  (let [test-data (double-array (range 100))
        _ (aset test-data 50 Double/NaN)
        stats-ary [:min :max :median :standard-deviation
                   :skew :quartile-1 :quartile-3]]
    (is (dfn/equals
         [0.0 99.0 49.0 29.159 5.3027E-4 24.0 75.0]
         (mapv (stats/descriptive-statistics stats-ary test-data)
               stats-ary)))))


(deftest percentiles
  (let [test-data (range 50)]
    (is (dfn/equals
         [0.0 11.75 24.5 37.25 49.0]
         (stats/quartiles test-data)))
    (let [test-fn (stats/quartile-outlier-fn test-data)]
      (is (= [true false false true]
             (mapv test-fn [-100 15 50 100]))))))
