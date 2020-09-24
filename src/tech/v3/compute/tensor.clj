(ns tech.v3.compute.tensor
  "Functions for dealing with tensors with the compute system"
  (:require [tech.v3.compute.driver :as drv]
            [tech.v3.compute.context :as compute-ctx]
            [tech.v3.datatype :as dtype]
            [tech.v3.tensor.dimensions :as dtt-dims]
            [tech.v3.tensor :as dtt])
  (:import [tech.v3.tensor Tensor]))


(defn new-tensor
  ([shape options]
   (let [{:keys [device]} (compute-ctx/options->context options)
         datatype (or (:datatype options) :float64)
         ecount (long (apply * shape))
         dev-buf (drv/allocate-device-buffer device ecount datatype options)]
     (dtt/construct-tensor dev-buf (dtt-dims/dimensions shape))))
  ([shape]
   (new-tensor shape {})))


(defn new-host-tensor
  ([shape options]
   (let [{:keys [driver]} (compute-ctx/options->context options)
         datatype (or (:datatype options) :float64)
         ecount (long (apply * shape))
         host-buf (drv/allocate-host-buffer driver ecount datatype options)]
     (dtt/construct-tensor host-buf (dtt-dims/dimensions shape))))
  ([shape]
   (new-host-tensor shape {})))


(defn assign!
  "assign rhs to lhs returning lhs"
  [lhs rhs & [options]]
  (let [lhs (dtt/ensure-tensor lhs)
        rhs (dtt/ensure-tensor rhs)
        lhs-buf (dtt/tensor->buffer lhs)
        rhs-buf (dtt/tensor->buffer rhs)
        lhs-shape (dtype/shape lhs)
        rhs-shape (dtype/shape rhs)
        stream (:stream (compute-ctx/options->context options))]
    (when-not (= (dtype/get-datatype lhs)
                 (dtype/get-datatype rhs))
      (throw (Exception. (format "Cannot assign tensors of different datatypes: %s %s"
                                 (dtype/get-datatype lhs)
                                 (dtype/get-datatype rhs)))))
    (when-not (and (= lhs-shape
                      rhs-shape))
      (throw (Exception. (format "Tensor shapes differ: %s %s"
                                 lhs-shape
                                 rhs-shape))))
    (when-not (and (dtt/simple-dimensions? lhs)
                   (dtt/simple-dimensions? rhs))
      (throw (Exception. "Both tensors must be 'simple' tensors.
no offset, no transpose, all data must be dense.")))
    (drv/copy-device->device stream
                             rhs-buf 0
                             lhs-buf 0
                             (dtype/ecount lhs-buf))
    (when (:sync? options)
      (drv/sync-with-host stream))
    lhs))


(defn clone-to-device
  "Clone a host tensor to a device.  Tensor must have relatively straighforward
  dimensions (transpose OK but arbitrary reorder or offset not OK) or :force?
  must be specified.
  options:
  :force?  Copy tensor to another buffer if necessary.
  :sync? Sync with stream to ensure copy operation is finished before moving forward."
  ([input-tens options]
   (let [input-tens (dtt/ensure-tensor input-tens)
         input-tens-buf (dtt/tensor->buffer input-tens)
         {:keys [device stream]} (compute-ctx/options->context options)
         datatype (dtype/get-datatype input-tens-buf)
         input-tens-buf (if (drv/acceptable-device-buffer? device input-tens-buf)
                          input-tens-buf
                          (dtype/make-container :native-buffer
                                                datatype
                                                input-tens-buf
                                                {:unchecked? true}))
         n-elems (dtype/ecount input-tens-buf)
         dev-buf (drv/allocate-device-buffer device n-elems datatype options)]
     (drv/copy-device->device stream
                              input-tens-buf 0
                              dev-buf 0
                              n-elems)
     (when (:sync? options)
       (drv/sync-with-host stream))
     (dtt/construct-tensor dev-buf (dtt/tensor->dimensions input-tens))))
  ([input-tens]
   (clone-to-device input-tens {})))


(defn ensure-device
  "Ensure a tensor can be used on a device.  Some devices can use CPU tensors."
  ([input-tens options]
   (let [device (or (:device options)
                    (compute-ctx/default-device))]
     (if (and (drv/acceptable-device-buffer? device input-tens)
              (dtt/dims-suitable-for-desc? input-tens))
       input-tens
       (clone-to-device input-tens))))
  ([input-tens]
   (ensure-device input-tens {})))


(defn ->tensor
  [data & [{:keys [datatype device stream sync?]
            :as options}]]
  (let [datatype (or datatype :float64)]
    (-> (dtt/->tensor data
                      :container-type :native-buffer
                      :datatype datatype)
        (ensure-device options))))


(defn clone-to-host
  "Copy this tensor to the host.  Synchronized by default."
  ([device-tens options]
   (let [options (update options
                         :sync?
                         #(if (nil? %) true %))
         driver (drv/get-driver device-tens)
         {:keys [stream]} (compute-ctx/options->context options)
         dev-buf (dtt/tensor->buffer device-tens)
         buf-elems (dtype/ecount dev-buf)
         host-buf (drv/allocate-host-buffer driver buf-elems
                                            (dtype/get-datatype dev-buf) options)]
     (drv/copy-device->device stream
                              dev-buf 0
                              host-buf 0
                              buf-elems)
     (when (:sync? options)
       (drv/sync-with-host stream))
     (dtt/construct-tensor host-buf (dtt/tensor->dimensions device-tens))))
  ([device-tens]
   (clone-to-host device-tens {:sync? true})))


(defn ensure-host
  "Ensure this tensor is a 'host' tensor.  Synchronized by default."
  ([device-tens options]
   (let [driver (drv/get-driver device-tens)]
     (if (drv/acceptable-host-buffer? driver device-tens)
       device-tens
       (clone-to-host device-tens options))))
  ([device-tens]
   (ensure-host device-tens {})))


(defn ->array
  [tens & [datatype]]
  (let [datatype (or datatype (dtype/get-datatype tens))
        tens (ensure-host tens)]
    (case datatype
      :int8 (dtype/->byte-array tens)
      :int16 (dtype/->short-array tens)
      :int32 (dtype/->int-array tens)
      :int64 (dtype/->long-array tens)
      :float32 (dtype/->float-array tens)
      :float64 (dtype/->double-array tens))))


(defn ->float-array
  ^floats [tens]
  (->array tens :float32))


(defn ->double-array
  ^floats [tens]
  (->array tens :float64))


(defn rows
  [tens]
  (dtt/rows tens))


(defn columns
  [tens]
  (dtt/columns tens))


(extend-type Tensor
  drv/PDriverProvider
  (get-driver [tens]
    (drv/get-driver (dtt/tensor->buffer tens)))
  drv/PDeviceProvider
  (get-device [tens]
    (drv/get-device (dtt/tensor->buffer tens))))
