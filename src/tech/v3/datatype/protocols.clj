(ns tech.v3.datatype.protocols
  (:import [tech.v3.datatype ElemwiseDatatype ECount Buffer BinaryBuffer]
           [clojure.lang Counted]
           [java.util List Map Set]
           [java.nio ByteOrder
            ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer CharBuffer]
           [org.roaringbitmap RoaringBitmap]))


(set! *warn-on-reflection* true)


(defprotocol PElemwiseDatatype
  (elemwise-datatype [item]))


(extend-protocol PElemwiseDatatype
  ElemwiseDatatype
  (elemwise-datatype [item] (.elemwiseDatatype item)))


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
  (sub-buffer [buffer offset length]
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
    "Offset this range by this offset.  Returns")
  (range->reverse-map [item]
    "Return a map whose keys are the values of the range
and whose values are the indexes that produce those values in the reader."))


(defprotocol PEndianness
  (endianness [item]
    "Either :little-endian or :big-endian"))


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


(defn platform-endianness
  []
  (if (= (ByteOrder/nativeOrder) ByteOrder/LITTLE_ENDIAN)
    :little-endian
    :big-endian))


(defprotocol PToBitmap
  (convertible-to-bitmap? [item])
  (as-roaring-bitmap ^{:tag RoaringBitmap} [item]))


(defprotocol PBitmapSet
  (set-and [lhs rhs])
  (set-and-not [lhs rhs])
  (set-or [lhs rhs])
  (set-xor [lhs rhs])
  (set-offset [item offset]
    "Offset a set by an amount")
  (set-add-range! [item start end])
  (set-add-block! [item data])
  (set-remove-range! [item start end])
  (set-remove-block! [item data]))


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
