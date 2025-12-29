(ns tech.v3.datatype.protocols
  (:require [ham-fisted.set :as set]
            [ham-fisted.defprotocol :refer [defprotocol extend extend-type extend-protocol]])
  (:import [tech.v3.datatype ElemwiseDatatype ECount Buffer BinaryBuffer
            ObjectReader]
           [clojure.lang Counted]
           [java.util List Map Set]
           [java.nio ByteOrder
            ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer CharBuffer])
  (:refer-clojure :exclude [defprotocol extend extend-type extend-protocol]))


(set! *warn-on-reflection* true)


(defprotocol PElemwiseDatatype
  (elemwise-datatype [item]))


(extend-protocol PElemwiseDatatype
  ElemwiseDatatype
  (elemwise-datatype [item] (.elemwiseDatatype item)))


(defprotocol POperationalElemwiseDatatype
  (operational-elemwise-datatype [item]
    "Some contains of a logical datatype represent themselves as a different
datatype for generic iteration.  For instance an `:int32` column with missing
values will represent itself as a `:float64` column.  This is a low-level
generic addition designed to enable safer naive uses of dataset columns
with missing values."))


(extend-type Object
  POperationalElemwiseDatatype
  (operational-elemwise-datatype [item]
    (elemwise-datatype item)))


(defprotocol PElemwiseCast
  (elemwise-cast [item new-dtype]))


(defprotocol PElemwiseReaderCast
  "Return a reader that correctly reads values of this new datatype.
  Note the returned reader need not report the new datatype.
  Defaults to:
```clojure
  (as-reader (elemwise-cast item new-dtype))
```"
  (^Buffer elemwise-reader-cast [item new-dtype]))


(defprotocol PDatatype
  (datatype [item]))


(defprotocol PReinterpretElemwiseCast
  "Return an object that reports its datatype as the new datatype.  This reinterprets
  the data in the container to be this type; it is not always possible to do this with
  all containers."
  (reinterpret-elemwise-cast [item new-dtype]))


(defprotocol PECount
  (^long ecount [item]))


(extend-protocol PECount
  nil
  (ecount [item] 0)
  Counted
  (ecount [item] (.count item))
  ECount
  (ecount [item] (.lsize item))
  List
  (ecount [item] (.size item))
  Map
  (ecount [item] (.size item))
  Set
  (ecount [item] (.size item)))


(defprotocol PShape
  (shape [item]))


(defprotocol PCopyRawData
  "Given a sequence of data copy it as fast as possible into a target item."
  (copy-raw->item! [raw-data ary-target target-offset options]))


(defprotocol PClone
  "Clone an object.  Implemented generically for all objects."
  (clone [item]))


(defprotocol PSetConstant
  (set-constant! [item offset elem-count value]))


(defprotocol PToNativeBuffer
  (convertible-to-native-buffer? [buf])
  (->native-buffer [buf]))


(defprotocol PToArrayBuffer
  (convertible-to-array-buffer? [buf])
  (->array-buffer [buf]))


(defprotocol PSubBuffer
  "Interface to create sub-buffers out of larger contiguous buffers."
  (sub-buffer [buffer ^long offset ^long length]
    "Create a sub buffer that shares the backing store with the main buffer."))


(defprotocol PToNDBufferDesc
  "Conversion to a buffer descriptor for consuming by an external C library."
  (convertible-to-nd-buffer-desc? [item])
  (->nd-buffer-descriptor [item]
    "Buffer descriptors are maps such that:
{:ptr com.sun.jna.Pointer that keeps reference back to original buffer.
 :datatype datatype of data that ptr points to.
 :device-type (optional) - one of #{:cpu :opencl :cuda}
 :shape -  vector of integers.
 :stride - vector of byte lengths for each dimension.
}
Note that this makes no mention of indianness; buffers are in the format of the host."))


(defprotocol PToBuffer
  (convertible-to-buffer? [item])
  (^Buffer ->buffer [item]))


(defprotocol PToWriter
  (convertible-to-writer? [item])
  (^Buffer ->writer [item]))

(defprotocol PToReader
  (convertible-to-reader? [item])
  (^Buffer ->reader [item]))


(defonce EMPTY-READER (reify ObjectReader (lsize [this] 0)))

(extend-type nil
  PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item] EMPTY-READER)
  PToReader
  (convertible-to-reader? [item] true)
  (->reader [item] EMPTY-READER)
  PToWriter
  (convertible-to-writer? [item] false)
  (->writer [item] EMPTY-READER)
  PToArrayBuffer
  (convertible-to-array-buffer? [item] false)
  PToNativeBuffer
  (convertible-to-native-buffer? [item] false))

(extend-type Object
  PToBuffer
  (convertible-to-buffer? [item] false)
  (->buffer [item] EMPTY-READER)
  PToReader
  (convertible-to-reader? [item] false)
  (->reader [item] EMPTY-READER)
  PToWriter
  (convertible-to-writer? [item] false)
  (->writer [item] EMPTY-READER)
  PToArrayBuffer
  (convertible-to-array-buffer? [item] false)
  PToNativeBuffer
  (convertible-to-native-buffer? [item] false)
  PToNDBufferDesc
  (convertible-to-nd-buffer-desc? [item] false))

(defprotocol PToBinaryBuffer
  (convertible-to-binary-buffer? [buf])
  (^BinaryBuffer ->binary-buffer [buf]))

(defprotocol POperator
  "It can be useful to know if a generic operator implements a higher level operation
  like :+"
  (op-name [item]))


(defprotocol PConstantTimeMinMax
  (has-constant-time-min-max? [item])
  (constant-time-min [item])
  (constant-time-max [item]))


(defprotocol PRangeConvertible
  (convertible-to-range? [item])
  (->range [item options]
    "Convert to something that implements the PRange protocols"))


(defprotocol PRange
  (range-select [lhs rhs]
    "Select the lhs range using the rhs range as an indexer.  Returns
  a new range as if the elements of lhs were indexed by rhs.")
  (range-start [item])
  (range-increment [item])
  (range-min [item])
  (range-max [item])
  (range-offset [item offset]
    "Offset this range by this offset.  Returns same length range with start,end offset."))


(defprotocol PEndianness
  (endianness [item]
    "Either :little-endian or :big-endian"))


(defprotocol PMemcpyInfo
  (memcpy-info [item]
    "Return a tuple of [object offset] used for unsafeCopyMemory call.
Only arraybuffers and native buffers need implement this pathway."))


(extend-protocol PEndianness
  ByteBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  ShortBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  IntBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  LongBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  FloatBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  DoubleBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian))
  CharBuffer
  (endianness [item]
    (if (.. item order (equals ByteOrder/BIG_ENDIAN))
      :big-endian
      :little-endian)))


(defn default-endianness
  [item]
  (or item :little-endian))


(defn platform-endianness []
  (if (identical? (ByteOrder/nativeOrder) ByteOrder/LITTLE_ENDIAN)
    :little-endian :big-endian))


(defprotocol PToBitmap
  (convertible-to-bitmap? [item])
  ;; type hints in protocols must have the full type declared else
  ;; deftype objects derived from them can get errors during AOT
  (as-roaring-bitmap ^{:tag org.roaringbitmap.RoaringBitmap} [item]))


(declare make-container)


(defmulti make-container
  (fn [container-type _datatype _options _elem-seq-or-count]
    container-type))


(defprotocol PToTensor
  (as-tensor [t]))


(defprotocol PTensor
  (reshape [t new-shape])
  (select [t select-args])
  (transpose [t reorder-vec])
  (broadcast [t new-shape])
  (rotate [t offset-vec])
  (^List slice [t n-dims right?])
  (mget [t idx-seq])
  (mset! [t idx-seq value]))

(defprotocol PComputeTensorData
  (compute-tensor-data [t]))

(defprotocol PApplyUnary
  (apply-unary-op [item un-op op-dtype res-dtype]))

(defprotocol PInputDatatype
  (input-datatype [this]))

(extend-protocol PInputDatatype
    nil (input-datatype [this] nil)
    Object (input-datatype [this] nil))

(extend-type Buffer
  PClone
  (clone [buf] (.cloneList buf))
  PToBuffer
  (convertible-to-buffer? [buf] true)
  (->buffer [buf] buf)
  PToReader
  (convertible-to-reader? [buf] (.allowsRead buf))
  (->reader [buf] buf)
  PToWriter
  (convertible-to-writer? [buf] (.allowsWrite buf))
  (->writer [buf] buf)
  PSubBuffer
  (sub-buffer [buf offset len]
    (let [offset (long offset)
          len (long len)])
    (.subBuffer buf offset (+ offset len)))
  PSetConstant
  (set-constant! [buf offset elem-count value]
    (let [offset (int offset)
          ec (int elem-count)]
      (.fillRange buf offset (+ offset ec) value))))


(defn set-and-not
  [l r]
  (set/difference l r))


(defn set-and
  [l r]
  (set/intersection l r))


(defn set-or
  [l r]
  (set/union l r))
