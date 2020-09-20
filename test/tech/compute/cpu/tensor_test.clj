(ns tech.compute.cpu.tensor-test
  (:require [tech.compute.cpu.driver :as cpu]
            [tech.compute.verify.tensor :as verify-tens]
            [clojure.test :refer :all]))


(deftest clone
  (verify-tens/clone (cpu/driver) :float64))


(deftest assign!
  (verify-tens/assign! (cpu/driver) :float64))
