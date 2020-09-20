(ns tech.compute.verify.driver
  (:require [clojure.test :refer :all]
            [tech.compute.driver :as drv]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.compute.verify.utils :as verify-utils]
            [tech.compute :as compute]
            [tech.compute.context :as compute-ctx]))


(defn simple-stream
  [driver datatype]
  (verify-utils/with-default-device-and-stream
    driver
    (let [{:keys [driver device stream]} (compute-ctx/options->context {})
          buf-a (compute/allocate-host-buffer driver 10 datatype)
          output-buf-a (compute/allocate-host-buffer driver 10 datatype)
          buf-b (compute/allocate-device-buffer device 10 datatype)
          input-data (dtype/make-container :typed-buffer datatype (range 10))
          output-data (dtype/make-container :typed-buffer datatype 10)]
      (dtype/copy! input-data buf-a)
      (dtype/set-value! buf-a 0 100.0)
      (dtype/copy! buf-a output-data)
      (compute/copy-device->device buf-a 0 buf-b 0 10)
      (compute/copy-device->device buf-b 0 output-buf-a 0 10)
      (compute/sync-with-host stream)
      (dtype/copy! output-buf-a output-data)
      (is (dfn/equals [100.0 1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0]
                      output-data)))))
