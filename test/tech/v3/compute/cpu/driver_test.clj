(ns tech.v3.compute.cpu.driver-test
  (:require [tech.v3.compute.cpu.driver :as cpu]
            [clojure.test :refer :all]
            [tech.v3.compute.verify.utils :refer [def-all-dtype-test
                                                def-double-float-test] :as test-utils]
            [tech.v3.compute.verify.driver :as verify-driver]))


(use-fixtures :each test-utils/test-wrapper)


(deftest simple-stream
  (verify-driver/simple-stream (cpu/driver) test-utils/*datatype*))
