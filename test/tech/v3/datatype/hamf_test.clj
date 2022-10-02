(ns tech.v3.datatype.hamf-test
  (:require [criterium.core :as crit]
            [clojure.test :refer [deftest is]]
            [ham-fisted.api :as hamf]
            [tech.v3.datatype :as dtype])
  (:import [tech.v3.datatype MutListBuffer]))




(comment

  (def data (hamf/double-array (hamf/range 10000)))
  (defn access-test
    [data]
    (let [rdr (dtype/->reader data)
          n-elems (dtype/ecount rdr)]
      (dotimes [idx 10000]
        (.readDouble rdr (rem idx n-elems)))))

  (crit/quick-bench (access-test (dtype/->reader data)))
  (crit/quick-bench (access-test (MutListBuffer. (hamf/->random-access data) true :float64)))

  )
