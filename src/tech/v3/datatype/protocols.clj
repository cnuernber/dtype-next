(ns tech.v3.datatype.protocols
  (:import [tech.v3.datatype ElemwiseDatatype Countable]
           [clojure.lang Counted]
           [java.util List Map Set]
           [java.nio ByteOrder
            ByteBuffer ShortBuffer IntBuffer LongBuffer
            FloatBuffer DoubleBuffer CharBuffer]))


(set! *warn-on-reflection* true)


(defprotocol PElemwiseDatatype
  (elemwise-datatype [item]))


(extend-protocol PElemwiseDatatype
  Object
  (elemwise-datatype [item] :object)
  ElemwiseDatatype
  (elemwise-datatype [item] (.elemwiseDatatype item)))


(defprotocol PContainerDatatype
  (container-datatype [item]))


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
  Countable
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


(defprotocol PPrototype
  (from-prototype [item datatype shape]))


(defprotocol PClone
  "Clone an object.  Implemented generically for all objects."
  (clone [item]))


(defprotocol PSetConstant
  (set-constant! [item offset value elem-count]))

(defprotocol PWriteIndexes
  (write-indexes! [item indexes values options]))

(defprotocol PReadIndexes
  (read-indexes! [item indexes values options]))

(defprotocol PRemoveRange
  (remove-range! [item idx n-elems]))

(defprotocol PInsertBlock
  (insert-block! [item idx values options]))


(defprotocol PToNativeBuffer
  (convertible-to-native-buffer? [buf])
  (->native-buffer [buf]))


(defprotocol PToArrayBuffer
  (convertible-to-array-buffer? [buf])
  (->array-buffer [buf]))


(defprotocol PBuffer
  "Interface to create sub-buffers out of larger contiguous buffers."
  (sub-buffer [buffer offset length]
    "Create a sub buffer that shares the backing store with the main buffer."))


(defprotocol PToList
  "Generically implemented for anything that implements ->array"
  (convertible-to-fastutil-list? [item])
  (->list-backing-store [item]))


(defn list-convertible?
  [item]
  (when (and item (convertible-to-fastutil-list? item))
    (convertible-to-fastutil-list? item)))


(defn as-list [item]
  (when (list-convertible? item)
    (->list-backing-store item)))


(defprotocol PToBufferDesc
  "Conversion to a buffer descriptor for consuming by an external C library."
  (convertible-to-buffer-desc? [item])
  (->buffer-descriptor [item]
    "Buffer descriptors are maps such that:
{:ptr com.sun.jna.Pointer that keeps reference back to original buffer.
 :datatype datatype of data that ptr points to.
 :device-type (optional) - one of #{:cpu :opencl :cuda}
 :shape -  vector of integers.
 :stride - vector of byte lengths for each dimension.
}
Note that this makes no mention of indianness; buffers are in the format of the host."))




;; Various other type conversions.  These happen quite a lot and we have found that
;; avoiding 'satisfies' is wise.  In all of these cases, options may contain at least
;; :datatype and :unchecked?
(defprotocol PToWriter
  (convertible-to-writer? [item])
  (->writer [item options]))

(defn as-writer
  [item & [options]]
  (when (convertible-to-writer? item)
    (->writer item options)))

(defprotocol PToReader
  (convertible-to-reader? [item])
  (->reader [item options]))

(defn as-reader
  [item & [options]]
  (when (convertible-to-reader? item)
    (->reader item options)))

(defprotocol POperator
  (op-name [item]))

(defprotocol PToUnaryOp
  (convertible-to-unary-op? [item])
  (->unary-op [item options]))

(defn as-unary-op
  [item & [options]]
  (when (convertible-to-unary-op? item)
    (->unary-op item options)))

(defprotocol PToUnaryBooleanOp
  (convertible-to-unary-boolean-op? [item])
  (->unary-boolean-op [item options]))

(defn as-unary-boolean-op
  [item & [options]]
  (when (convertible-to-unary-boolean-op? item)
    (->unary-boolean-op item options)))

(defprotocol PToBinaryOp
  (convertible-to-binary-op? [item])
  (->binary-op [item options]))

(defn as-binary-op
  [item & [options]]
  (when (convertible-to-binary-op? item)
    (->binary-op item options)))

(defprotocol PToBinaryBooleanOp
  (convertible-to-binary-boolean-op? [item])
  (->binary-boolean-op [item options]))

(defn as-binary-boolean-op
  [item & [options]]
  (when (convertible-to-binary-boolean-op? item)
    (->binary-boolean-op item options)))


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


(declare make-container)


(defmulti make-container
  (fn [container-type _datatype _elem-seq-or-count _options]
    container-type))
