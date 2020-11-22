(ns tech.v3.datatype.nippy
  "Nippy bindings for datatype base types and tensor types"
  (:require [taoensso.nippy :as nippy]
            [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.array-buffer :as array-buffer]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.tensor :as dtt]
            [tech.v3.tensor.dimensions :as dims])
  (:import [tech.v3.datatype Buffer]
           [tech.v3.datatype.array_buffer ArrayBuffer]
           [tech.v3.datatype.native_buffer NativeBuffer]
           [tech.v3.tensor Tensor]))


(defn buffer->data
  [ary-buf]
  {:datatype (dtype-base/elemwise-datatype ary-buf)
   :data (dtype-cmc/->array ary-buf)
   :metadata (meta ary-buf)})


(defn data->buffer
  [{:keys [datatype data metadata]}]
  (with-meta
    (array-buffer/array-buffer data datatype)
    metadata))


(nippy/extend-freeze
 ArrayBuffer :tech.v3.datatype/buffer
 [buf out]
 (nippy/-freeze-without-meta! (buffer->data buf) out))


(nippy/extend-thaw
 :tech.v3.datatype/buffer
 [in]
 (-> (nippy/thaw-from-in! in)
     (data->buffer)))


(nippy/extend-freeze
 NativeBuffer :tech.v3.datatype/buffer
 [buf out]
 (nippy/-freeze-without-meta! (buffer->data buf) out))


(nippy/extend-freeze
 Buffer :tech.v3.datatype/buffer
 [buf out]
 (nippy/-freeze-without-meta! (buffer->data buf) out))


(defn tensor->data
  [tensor]
  {:shape (dtype-base/shape tensor)
   :metadata (meta tensor)
   :buffer (buffer->data (dtype-base/->buffer tensor))})


(defn data->tensor
  [{:keys [shape buffer metadata]}]
  (dtt/construct-tensor (data->buffer buffer)
                        (dims/dimensions shape)
                        metadata))


(nippy/extend-freeze
 Tensor :tech.v3/tensor
 [buf out]
 (nippy/-freeze-without-meta! (tensor->data buf) out))


(nippy/extend-thaw
 :tech.v3/tensor
 [in]
 (-> (nippy/thaw-from-in! in)
     (data->tensor)))
