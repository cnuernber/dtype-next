(ns tech.v3.compute
  (:require [tech.v3.compute.driver :as drv]
            [tech.v3.compute.registry :as registry]
            [tech.v3.compute.context :as compute-ctx]
            [tech.v3.datatype :as dtype]
            [clojure.test :refer :all]
            [tech.resource :as resource]))


(defn driver-names
  "Get the names of the registered drivers."
  []
  (registry/driver-names))


(defn driver
  "Do a registry lookup to find a driver by its name."
  [driver-name]
  (registry/driver driver-name))


(defn ->driver
  "Generically get a driver from a thing"
  [item]
  (drv/get-driver item))


(defn ->device
  "Generically get a device from a thing"
  [item]
  (drv/get-device item))


(defn ->stream
  "Generically get a stream from a thing"
  [item]
  (drv/get-stream item))


(defn driver-name
  [driver]
  (drv/driver-name driver))


(defn get-devices
  [driver]
  (drv/get-devices driver))


(defn default-device
  [driver]
  (first (get-devices driver)))


(defn allocate-host-buffer
  "Allocate a host buffer.  Usage type gives a hint as to the
intended usage of the buffer."
  [driver elem-count elem-type & {:keys [usage-type]
                                  :or {usage-type :one-time}
                                  :as options}]
  (drv/allocate-host-buffer driver elem-count
                            elem-type (assoc options
                                             :usage-type usage-type)))


;; Device API

(defn supports-create-stream?
  "Does this device support create-stream?"
  [device]
  (drv/supports-create-stream? device))

(defn default-stream
  "All devices must have a default stream whether they support create or not."
  [device]
  (drv/default-stream device))

(defn create-stream
  "Create a stream of execution.  Streams are indepenent threads of execution.  They can
  be synchronized with each other and the main thread using events."
  [device]
  (drv/create-stream device))


(defn allocate-device-buffer
  "Allocate a device buffer.  This is the generic unit of data storage used for
  computation.  No options at this time."
  [device elem-count elem-type & {:as options}]
  (drv/allocate-device-buffer device elem-count elem-type options))

;;Stream API

(defn- check-legal-copy!
  [src-buffer src-offset dst-buffer dst-offset elem-count]
  (let [src-len (- (dtype/ecount src-buffer) (long src-offset))
        dst-len (- (dtype/ecount dst-buffer) (long dst-offset))
        elem-count (long elem-count)]
    (when (> elem-count src-len)
      (throw (ex-info "Copy out of range"
                      {:src-len src-len
                       :elem-count elem-count})))
    (when (> elem-count dst-len)
      (throw (ex-info "Copy out of range"
                      {:dst-len dst-len
                       :elem-count elem-count})))))


(defn- provided-or-default-stream
  [stream device-buffer]
  (or stream
      (:stream compute-ctx/*context)
      (default-stream (->device device-buffer))))


(defn device->device-copy-compatible?
  [src-device dst-device]
  (drv/device->device-copy-compatible? src-device dst-device))


(defn copy-device->device
  "Copy from one device to another.  If no stream is provided then the destination
buffer's device's default stream is used."
  [dev-a dev-a-off dev-b dev-b-off elem-count & {:keys [stream] :as options}]
  (check-legal-copy! dev-a dev-a-off dev-b dev-b-off elem-count)
  (let [{:keys [stream]} (compute-ctx/options->context options)]
    (drv/copy-device->device stream dev-a dev-a-off dev-b dev-b-off elem-count)))


(defn sync-with-host
  "Block host until stream's queue is finished executing"
  ([stream]
   (drv/sync-with-host stream))
  ([]
   (sync-with-host (compute-ctx/default-stream))))

(defn sync-with-stream
  "Create an event in src-stream's execution queue, then have dst stream wait on that
  event.  This allows dst-stream to ensure src-stream has reached a certain point of
  execution before continuing.  Both streams must be of the same driver."
  [src-stream dst-stream & [options]]
  (drv/sync-with-stream src-stream dst-stream))
