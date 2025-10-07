(ns tech.v3.libs.neanderthal-post-48
  "Implementation of the various datatype protocols for neanderthal datatypes.  Users
  must require this to enable ->reader and as-tensor functionality for neanderthal
  datatypes."
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.hamf-proto :as hamf-proto]
            [ham-fisted.defprotocol :as hamf-defproto]
            [tech.v3.datatype.native-buffer :as nb]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.tensor :as dtt]
            [tech.v3.libs.javacpp]
            [uncomplicate.commons.core :as n-core]
            [uncomplicate.neanderthal.core :as nean]
            [uncomplicate.neanderthal.native :as n-native]
            ;;This only exists after the javacpp switch - here to cause compilation issue
            [uncomplicate.clojure-cpp])
  (:import [uncomplicate.neanderthal.internal.api NativeBlock]))


(comment
  (require '[uncomplicate.neanderthal.core :as nean])
  (def a (n-native/dge 3 3 (range 9)))
  (def dg (nean/dia a)) ;;strided vector
  (def a1 (nean/submatrix a 2 3))
  (def a1 (nean/submatrix a 3 2))
  (def aa (nean/submatrix a 2 2))
  (def a2 (nean/submatrix a 1 2))
  )

(hamf-defproto/extend-type NativeBlock
  hamf-proto/PElemwiseDatatype
  (elemwise-datatype [b] (condp = (:entry-type (n-core/info b))
                           Double/TYPE :float64
                           Float/TYPE :float32
                           Integer/TYPE :int32))
  hamf-proto/PECount
  (ecount [item] (long (:dim (n-core/info item)))))

(extend-type NativeBlock
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [a]
    (let [info (n-core/info a)
          region (get info :region)]
      (identical? :cpu (get info :device))))
  (->native-buffer [a] (dtype-proto/->native-buffer (.buffer a)))
  dtype-proto/PShape
  (shape [a]
    (let [info (n-core/info a)]
      (if (contains? info :matrix-type)
        [(:m info) (:n info)]
        [(:dim info)])))
  dtype-proto/PToNDBufferDesc
  (convertible-to-nd-buffer-desc? [item] (dtype-proto/convertible-to-native-buffer? item))
  (->nd-buffer-descriptor [item]
    (let [item-info (n-core/info item)
          item-dtype (hamf-proto/elemwise-datatype item)
          item-shape (dtype-proto/shape item)
          nb (-> (dtype-proto/->native-buffer item)
                 (nb/set-gc-obj item))
          strides (if (== 2 (count item-shape))
                    [(get item-info :stride) 1]
                    [(get item-info :stride)])
          strides (if (identical? :column (get-in item-info [:storage :layout]))
                    (vec (reverse strides))
                    strides)
          byte-size (casting/numeric-byte-width item-dtype)]
      {:ptr (.address nb)
       :elemwise-datatype item-dtype
       :datatype {:container-type :tensor
                  :elemwise-datatype item-dtype}
       :endianness (.endianness nb)
       :shape item-shape
       ;;TVM needs the device type
       :device-type :cpu
       :strides (mapv #(* % byte-size) strides)
       :native-buffer nb}))
  dtype-proto/PToTensor
  (as-tensor [item]
    (when (dtype-proto/convertible-to-nd-buffer-desc? item)
      (-> (dtype-proto/->nd-buffer-descriptor item)
          (dtt/nd-buffer-descriptor->tensor)))))
