(ns tech.compute.cpu.driver
  (:require [tech.compute.driver :as drv]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.protocols :as dtype-proto]
            [clojure.core.async :as async]
            [tech.resource :as resource]
            [tech.resource.stack :as stack]
            [tech.compute :as compute]
            [tech.compute.registry :as registry]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* true)


(defprotocol PToCPUStream
  ;;Setup so other things can masquerade as a cpu stream as long as they
  ;;have a conversion to a CPUStream record.
  (->cpu-stream [item]))


(defrecord CPUDevice [driver-fn device-id error-atom default-stream])
(defrecord CPUStream [device-fn input-chan exit-chan error-atom]
  PToCPUStream
  (->cpu-stream [item] item))
(defrecord CPUDriver [devices error-atom])


(defonce driver-name (registry/current-ns->keyword))


(extend-protocol drv/PDriverProvider
  CPUDriver
  (get-driver [driver] driver)
  CPUDevice
  (get-driver [device] ((get device :driver-fn)))
  CPUStream
  (get-driver [stream] (compute/->driver ((:device-fn stream)))))


(extend-protocol drv/PDeviceProvider
  CPUDriver
  (get-device [driver] (compute/default-device driver))
  CPUDevice
  (get-device [device] device)
  CPUStream
  (get-device [stream] ((:device-fn stream))))


(extend-protocol drv/PStreamProvider
  CPUStream
  (get-stream [stream] stream))


(extend-type CPUStream
  stack/PResource
  (release-resource [impl]
    (when (.input-chan impl)
      (async/close! (.input-chan impl)))))


(defn get-memory-info
  []
  {:free (.freeMemory (Runtime/getRuntime))
   :total (.totalMemory (Runtime/getRuntime))})


(defn cpu-stream
  ([device error-atom]
   (let [^CPUStream retval (->CPUStream (constantly device) (async/chan 16)
                                        (async/chan) error-atom)]
     (async/thread
       (loop [next-val (async/<!! (:input-chan retval))]
         (when next-val
           (try
             (next-val)
             (catch Throwable e
               (reset! error-atom e)))
           (recur (async/<!! (:input-chan retval)))))
       (async/close! (:exit-chan retval)))
     (resource/track retval)))
  ([device] (cpu-stream device (atom nil))))

(declare driver)

(defn main-thread-cpu-stream
  "Create a cpu stream that will execute everything immediately inline.
Use with care; the synchonization primitives will just hang with this stream."
  ^CPUStream []
  (->CPUStream (constantly (compute/default-device (driver))) nil nil nil))


(defn is-main-thread-cpu-stream?
  [stream]
  (let [^CPUStream stream (->cpu-stream stream)]
    (not (or (.input-chan stream)
             (.exit-chan stream)
             (.error-atom stream)))))


(defn is-thread-cpu-stream?
  [^CPUStream stream]
  (not (is-main-thread-cpu-stream? stream)))


(defn- check-stream-error-atom
  [item]
  (when-let [error-atom (:error-atom item)]
    (let [error @error-atom]
      (when error
        (compare-and-set! error-atom error nil)
        (throw error)))))


(defn check-stream-error
  [item]
  (check-stream-error-atom (->cpu-stream item)))


(defmacro with-stream-dispatch
  [stream & body]
  `(if (is-thread-cpu-stream? ~stream)
     (do
       (check-stream-error ~stream)
       (let [^CPUStream stream# (->cpu-stream ~stream)]
         (async/>!! (.input-chan stream#)
                    (fn [] ~@body))))
     (do
       ~@body)))


(defrecord CPUEvent [input-chan])


(extend-type CPUStream
  drv/PStream
  (copy-host->device [stream host-buffer host-offset
                      device-buffer device-offset elem-count]
    (with-stream-dispatch stream
      (dtype/copy!
       (dtype/sub-buffer host-buffer host-offset elem-count)
       (dtype/sub-buffer device-buffer device-offset elem-count))))
  (copy-device->host [stream device-buffer device-offset host-buffer
                      host-offset elem-count]
    (with-stream-dispatch stream
      (dtype/copy!
       (dtype/sub-buffer device-buffer device-offset elem-count)
       (dtype/sub-buffer host-buffer host-offset elem-count))))
  (copy-device->device [stream dev-a dev-a-off dev-b dev-b-off elem-count]
    (with-stream-dispatch stream
      (dtype/copy!
       (dtype/sub-buffer dev-a dev-a-off elem-count)
       (dtype/sub-buffer dev-b dev-b-off elem-count))))
  (sync-with-host [stream]
    ;;If main thread cpu stream then we are already syncced
    (when-not (is-main-thread-cpu-stream? stream)
      (let [^CPUEvent event (->CPUEvent (async/chan))]
        (with-stream-dispatch stream
          (async/close! (.input-chan event)))
        (async/<!! (.input-chan event)))))
  (sync-with-stream [src-stream dst-stream]
    (let [^CPUEvent event (->CPUEvent (async/chan))]
      (with-stream-dispatch src-stream
        (async/close! (.input-chan event)))
      (with-stream-dispatch dst-stream
        (async/<!! (.input-chan event))))))


(defn make-cpu-device
  [driver-fn dev-number error-atom]
  (let [retval (->CPUDevice driver-fn dev-number error-atom (atom nil))
        ;;Default cpu stream runs in the main thread of execution
        default-stream (->CPUStream (constantly retval) nil nil nil)]
    (reset! (:default-stream retval) default-stream)
    retval))


(extend-type CPUDevice
  drv/PDevice

  (memory-info [impl]
    (get-memory-info))

  (supports-create-stream? [device] true)

  (default-stream [device] @(:default-stream device))

  (create-stream [impl]
    (check-stream-error-atom impl)
    (cpu-stream impl (:error-atom impl)))

  (allocate-device-buffer [impl elem-count elem-type options]
    (check-stream-error-atom impl)
    (dtype/make-container :native-buffer elem-type options elem-count))

  (acceptable-device-buffer? [device item]
    (dtype-proto/convertible-to-writer? item))

  (device->device-copy-compatible? [src-device dst-device] nil))


(extend-type CPUDriver
  drv/PDriver
  (driver-name [impl]
    driver-name)

  (get-devices [impl]
    @(get impl :devices))

  (allocate-host-buffer [impl elem-count elem-type options]
    (check-stream-error-atom impl)
    (dtype/make-container :native-buffer elem-type options elem-count))

  (acceptable-host-buffer? [impl item]
    (dtype-proto/convertible-to-writer? item)))


(declare default-cpu-stream)


(def driver
  (memoize
   (fn []
     (let [error-atom (atom nil)
           retval (->CPUDriver (atom nil) error-atom)]
       (reset! (get retval :devices)
               (->> (range 1)
                    (mapv #(make-cpu-device (constantly retval) % error-atom))))
       retval))))


(registry/register-driver (driver))
(registry/set-cpu-driver-name! (-> (driver)
                                   drv/driver-name))


(extend-type Object
  drv/PDriverProvider
  (get-driver [impl] (driver))
  drv/PDeviceProvider
  (get-device [impl]
    (first (drv/get-devices (driver)))))
