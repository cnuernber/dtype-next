(ns tech.compute.cpu.driver-test
  (:require [tech.compute.cpu.driver :as cpu]
            [clojure.test :refer :all]
            [tech.compute.verify.utils :refer [def-all-dtype-test
                                                def-double-float-test] :as test-utils]
            [tech.compute.verify.driver :as verify-driver]))


(use-fixtures :each test-utils/test-wrapper)


(deftest simple-stream
  (verify-driver/simple-stream (cpu/driver) test-utils/*datatype*))
