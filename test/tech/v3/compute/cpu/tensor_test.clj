(ns tech.v3.compute.cpu.tensor-test
  (:require [tech.v3.compute.cpu.driver :as cpu]
            [tech.v3.compute.verify.tensor :as verify-tens]
            [clojure.test :refer :all]))


(deftest clone
  (verify-tens/clone (cpu/driver) :float64))


(deftest assign!
  (verify-tens/assign! (cpu/driver) :float64))
