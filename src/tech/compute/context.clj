(ns tech.compute.context
  "Compute context allows someone to setup the default driver, device, and stream to
  use in function calls."
  (:require [tech.compute.registry :as registry]
            [tech.compute.driver :as drv]))


(def ^:dynamic *context {})


(defn default-driver
  []
  (or (:driver *context)
      (registry/driver @registry/*cpu-driver-name*)))


(defn default-device
  []
  (let [retval
        (or (:device *context)
            (first (drv/get-devices (default-driver))))]
    (when-not retval
      (throw (Exception.
              (format "%s: No devices found"
                      (drv/driver-name (default-driver))))))
    retval))


(defn default-stream
  []
  (or (:stream *context)
      (drv/default-stream (default-device))))


(defmacro with-context
  [context & body]
  `(with-bindings {#'*context ~context}
     ~@body))


(defmacro with-merged-context
  [context & body]
  `(with-context
     (merge *context ~context)
     ~@body))


(defn default-context
  []
  {:driver (default-driver)
   :device (default-device)
   :stream (default-stream)})


(defn options->context
  "Given an options map, return a augmented map that always includes
  device, driver, and stream."
  [opt-map]
  (merge (default-context) opt-map))
