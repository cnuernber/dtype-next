(ns tech.v3.datatype.index-algebra-test
  (:require [tech.v3.datatype.index-algebra :as idx-alg]
            [tech.v3.datatype.protocols :as dtype-proto]
            ;;This has to be loaded to fill out the protocol matrix.
            [tech.v3.datatype :as dtype])
  (:require [clojure.test :refer :all]))




(deftest idx-alg-test
  (let [item (-> [1 2 3 4]
                 (idx-alg/offset 2)
                 (idx-alg/broadcast 8)
                 (idx-alg/select (range 1 5)))]
    (is (= [4 1 2 3] (vec item)))
    (is (= (vec (flatten (repeat 2 [4 1 2 3])))
           (idx-alg/broadcast item 8)))
    (let [new-item (-> (idx-alg/broadcast item 8)
                       (idx-alg/offset 3)
                       (idx-alg/select (range 2 6)))]
      (is (= [1 2 3 4] new-item))
      (is (dtype-proto/convertible-to-range? new-item)))
    (let [new-item (idx-alg/select item 2)]
      (is (= [2] new-item))
      (is (:select-scalar? (meta new-item))))))
