(ns tech.v3.datatype.array-buffer
  (:require [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.typecast :as typecast]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.packing :as packing]
            [tech.v3.datatype.pprint :as dtype-pp]
            [primitive-math :as pmath])
  (:import [clojure.lang IObj Counted Indexed IFn]
           [tech.v3.datatype Buffer ArrayHelpers BufferCollection BinaryBuffer
            ByteConversions]
           [java.util Arrays]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)


(defmacro java-array-buffer->io
  [datatype cast-dtype advertised-datatype buffer java-ary offset n-elems]
  `(let [~'java-ary (typecast/datatype->array ~datatype ~java-ary)
         {~'unpacking-read :unpacking-read
          ~'packing-write :packing-write} (packing/buffer-packing-pair
                                           ~advertised-datatype)]
     (reify
       dtype-proto/PToArrayBuffer
       (convertible-to-array-buffer? [this#] true)
       (->array-buffer [this#] ~buffer)
       dtype-proto/PToBinaryBuffer
       (convertible-to-binary-buffer? [this#]
         (dtype-proto/convertible-to-binary-buffer? ~buffer))
       (->binary-buffer [this#]
         (dtype-proto/->binary-buffer ~buffer))
       ;;Forward protocol methods that are efficiently implemented by the buffer
       dtype-proto/PSubBuffer
       (sub-buffer [this# offset# length#]
         (-> (dtype-proto/sub-buffer ~buffer offset# length#)
             (dtype-proto/->reader)))
       dtype-proto/PDatatype
       (datatype [this#]
         (dtype-proto/datatype ~buffer))
       dtype-proto/PElemwiseReaderCast
       (elemwise-reader-cast [this# new-dtype#] this#)
       ~(typecast/datatype->io-type (casting/safe-flatten cast-dtype))
       (elemwiseDatatype [rdr#] ~advertised-datatype)
       (lsize [rdr#] ~n-elems)
       (allowsRead [rdr#] true)
       (allowsWrite [rdr#] true)
       ~@(cond
           (= cast-dtype :boolean)
           [`(readBoolean [rdr# ~'idx]
                          (aget ~'java-ary (pmath/+ ~offset ~'idx)))]
           ;;For integer types, everything implements readlong.
           ;;They also implement readX where X maps to exactly the datatype.
           ;;For example byte arrays implement readLong and readByte.
           (casting/integer-type? cast-dtype)
           (concat
            [`(readLong [rdr# ~'idx]
                        (casting/datatype->unchecked-cast-fn
                         ~cast-dtype :int64
                         (casting/datatype->unchecked-cast-fn
                          ~datatype ~cast-dtype
                          (aget ~'java-ary (pmath/+ ~offset ~'idx)))))
             `(accumPlusLong
               [rdr# idx# value#]
               (ArrayHelpers/accumPlus ~'java-ary (pmath/+ ~offset idx#)
                                       (->> value#
                                            (casting/datatype->unchecked-cast-fn
                                             :int64 ~cast-dtype)
                                            (casting/datatype->unchecked-cast-fn
                                             ~cast-dtype ~datatype))))]
            (when-not (or (= :int64 cast-dtype)
                          (= :uint32 cast-dtype)
                          (= :uint64 cast-dtype))
              ;;Exact reader fns for the exact datatype
              [(cond
                 (= cast-dtype :int8)
                 `(readByte [rdr# ~'idx]
                            (aget ~'java-ary (pmath/+ ~offset ~'idx)))
                 (or (= cast-dtype :uint8)
                     (= cast-dtype :int16))
                 `(readShort [rdr# ~'idx]
                             (casting/datatype->unchecked-cast-fn
                              ~cast-dtype :int16
                              (casting/datatype->unchecked-cast-fn
                               ~datatype ~cast-dtype
                               (aget ~'java-ary (pmath/+ ~offset ~'idx)))))
                 (= cast-dtype :char)
                 `(readChar [rdr# ~'idx]
                            (aget ~'java-ary (pmath/+ ~offset ~'idx)))
                 (or (= cast-dtype :uint16)
                     (= cast-dtype :int32))
                 `(readInt [rdr# ~'idx]
                           (casting/datatype->unchecked-cast-fn
                            ~cast-dtype :int32
                            (casting/datatype->unchecked-cast-fn
                             ~datatype ~cast-dtype
                             (aget ~'java-ary (pmath/+ ~offset ~'idx)))))
                 :else (throw (Exception. (format "Macro expansion error-%s"
                                                  cast-dtype))))])
            (if (= :char cast-dtype)
              [`(readObject [rdr# ~'idx]
                            (.readChar rdr# ~'idx))]
              ;;Integer types may be representing packed objects
              [`(readObject [~'rdr ~'idx]
                            (if ~'unpacking-read
                              (~'unpacking-read ~'rdr ~'idx)
                              (.readLong ~'rdr ~'idx)))]))
           (casting/float-type? cast-dtype)
           [`(readDouble [rdr# ~'idx]
                         (casting/datatype->unchecked-cast-fn
                          ~datatype :float64
                          (aget ~'java-ary (pmath/+ ~offset ~'idx))))
            `(readFloat [rdr# ~'idx]
                        (casting/datatype->unchecked-cast-fn
                         ~datatype :float32
                         (aget ~'java-ary (pmath/+ ~offset ~'idx))))
            `(accumPlusDouble
              [rdr# idx# value#]
              (ArrayHelpers/accumPlus ~'java-ary (pmath/+ idx# ~offset)
                                      (casting/datatype->unchecked-cast-fn
                                       ~datatype ~cast-dtype value#)))]
           :else
           [`(readObject [rdr# ~'idx]
                         (casting/datatype->unchecked-cast-fn
                          ~cast-dtype :object
                          (casting/datatype->unchecked-cast-fn
                           ~datatype ~cast-dtype
                           (aget ~'java-ary (pmath/+ ~offset ~'idx)))))])
       ~@(cond
           (= :boolean cast-dtype)
           [`(writeBoolean [wtr# idx# val#]
                           (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset idx#) val#))]
           (casting/integer-type? cast-dtype)
           (concat
            [`(writeLong [rdr# ~'idx ~'value]
                         (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset ~'idx)
                               (casting/datatype->unchecked-cast-fn
                                ~cast-dtype ~datatype
                                (casting/datatype->cast-fn
                                 :int64 ~cast-dtype ~'value))))]
            (when-not (or (= :int64 cast-dtype)
                          (= :uint32 cast-dtype)
                          (= :uint64 cast-dtype))
              ;;Exact reader fns for the exact datatype
              [(cond
                 (= cast-dtype :int8)
                 `(writeByte [rdr# ~'idx ~'value]
                            (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset ~'idx) ~'value))
                 (or (= cast-dtype :uint8)
                     (= cast-dtype :int16))
                 `(writeShort [rdr# ~'idx ~'value]
                              (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset ~'idx)
                                    (casting/datatype->unchecked-cast-fn
                                     ~cast-dtype ~datatype
                                     (casting/datatype->cast-fn
                                      :int16 ~cast-dtype ~'value))))
                 (= cast-dtype :char)
                 `(writeChar [rdr# ~'idx ~'value]
                             (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset ~'idx) ~'value))
                 (or (= cast-dtype :uint16)
                     (= cast-dtype :int32))
                 `(writeInt [rdr# ~'idx ~'value]
                            (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset ~'idx)
                                  (casting/datatype->unchecked-cast-fn
                                   ~cast-dtype ~datatype
                                   (casting/datatype->cast-fn
                                    :int32 ~cast-dtype ~'value))))
                 :else (throw (Exception. (format "Macro expansion error-%s"
                                                  cast-dtype))))])
            ;;Overload the writeObject pathway to use writeChar instead of writeLong
            (if (= :char cast-dtype)
              [`(writeObject [rdr# ~'idx ~'value]
                             (.writeChar rdr# ~'idx (char ~'value)))]
              [`(writeObject [~'rdr ~'idx ~'value]
                             (if ~'packing-write
                               (~'packing-write ~'rdr ~'idx ~'value)
                               (.writeLong ~'rdr ~'idx (long ~'value))))]))
           (casting/float-type? cast-dtype)
           [`(writeDouble [rdr# ~'idx ~'value]
                          (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset ~'idx)
                                (casting/datatype->unchecked-cast-fn
                                 :float64 ~cast-dtype ~'value)))
            `(writeFloat [rdr# ~'idx ~'value]
                         (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset ~'idx)
                               (casting/datatype->unchecked-cast-fn
                                :float32 ~cast-dtype ~'value)))]
           :else
           [`(writeObject [wtr# idx# val#]
                          ;;Writing values is always checked, no options.
                          (ArrayHelpers/aset ~'java-ary (pmath/+ ~offset idx#) val#))]))))


(declare array-buffer->io)


(deftype ArrayBuffer [ary-data ^long offset ^long n-elems datatype metadata
                      ^:volatile-mutable ^Buffer cached-io]
  dtype-proto/PElemwiseDatatype
  (elemwise-datatype [item] datatype)
  dtype-proto/PDatatype
  (datatype [item] :array-buffer)
  dtype-proto/PECount
  (ecount [item] n-elems)
  dtype-proto/PEndianness
  (endianness [item] :little-endian)
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [item new-dtype]
    (or cached-io (dtype-proto/->reader item)))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [item] true)
  (->array-buffer [item] item)
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [item]
    (and
     (= datatype :int8)
     (dtype-proto/convertible-to-binary-buffer? ary-data)))
  (->binary-buffer [item]
    (-> (dtype-proto/->binary-buffer ary-data)
        (dtype-proto/sub-buffer offset n-elems)))
  dtype-proto/PSubBuffer
  (sub-buffer [item off len]
    (ArrayBuffer. ary-data
                  (+ offset (int off))
                  (int len)
                  datatype
                  metadata
                  nil))
  dtype-proto/PSetConstant
  (set-constant! [this off elem-count value]
    (let [offset (+ offset (int off))
          elem-count (int elem-count)
          end-offset (+ elem-count offset)
          value (casting/cast value datatype)]
      ;;arrays/fill is very, very fast
      (case (dtype-proto/elemwise-datatype ary-data)
        :boolean (Arrays/fill ^booleans ary-data offset end-offset (boolean value))
        :int8 (Arrays/fill ^bytes ary-data offset end-offset (unchecked-byte value))
        :int16 (Arrays/fill ^shorts ary-data offset end-offset (unchecked-short value))
        :char (Arrays/fill ^chars ary-data offset end-offset (unchecked-char value))
        :int32 (Arrays/fill ^ints ary-data offset end-offset (unchecked-int value))
        :int64 (Arrays/fill ^longs ary-data offset end-offset (unchecked-long value))
        :float32 (Arrays/fill ^floats ary-data offset
                              end-offset (unchecked-float value))
        :float64 (Arrays/fill ^longs ary-data offset end-offset (unchecked-long value))
        (Arrays/fill ^"[Ljava.lang.Object;" ary-data  offset end-offset value))))
  dtype-proto/PClone
  (clone [this]
    (dtype-proto/make-container :jvm-heap datatype {} this))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [item] true)
  (->buffer [item]
    (if cached-io cached-io
        (let [io
              (array-buffer->io ary-data datatype item offset n-elems)]
          (set! cached-io io)
          io)))
  dtype-proto/PToReader
  (convertible-to-reader? [item] true)
  (->reader [item]
    (dtype-proto/->buffer item))
  dtype-proto/PToWriter
  (convertible-to-writer? [item] true)
  (->writer [item]
    (dtype-proto/->buffer item))
  IObj
  (meta [item] metadata)
  (withMeta [item metadata]
    (ArrayBuffer. ary-data offset n-elems datatype metadata cached-io))
  Counted
  (count [item] (int (dtype-proto/ecount item)))
  Indexed
  (nth [item idx]
    ((dtype-proto/->buffer item) idx))
  (nth [item idx def-val]
    (if (and (>= idx 0) (< idx (.count item)))
      ((dtype-proto/->buffer item) idx)
      def-val))
  IFn
  (invoke [item idx]
    (nth item (int idx)))
  (invoke [item idx value]
    ((dtype-proto/->writer item) idx value))
  (applyTo [item argseq]
    (case (count argseq)
      1 (.invoke item (first argseq))
      2 (.invoke item (first argseq) (second argseq))))
  BufferCollection
  (iterator [this]
    (dtype-proto/->buffer this)
    (.iterator cached-io))
  (size [this] (int (dtype-proto/ecount this)))
  (toArray [this]
    (dtype-proto/->buffer this)
    (.toArray cached-io))
  Object
  (toString [item]
    (dtype-pp/buffer->string item "array-buffer")))


(dtype-pp/implement-tostring-print ArrayBuffer)


(casting/add-object-datatype! :array-buffer ArrayBuffer false)


(defn- array-buffer->io
  [ary-data datatype ^ArrayBuffer item offset n-elems]
  (let [offset (long offset)
        n-elems (long n-elems)]
    (case [(dtype-proto/elemwise-datatype ary-data)
           (casting/un-alias-datatype datatype)]
      [:boolean :boolean] (java-array-buffer->io :boolean :boolean datatype item
                                                 ary-data offset n-elems)
      [:int8 :uint8] (java-array-buffer->io :int8 :uint8 datatype item
                                            ary-data offset n-elems)
      [:int8 :int8] (java-array-buffer->io :int8 :int8 datatype item
                                           ary-data offset n-elems)
      [:int16 :uint16] (java-array-buffer->io :int16 :uint16 datatype item
                                              ary-data offset n-elems)
      [:int16 :int16] (java-array-buffer->io :int16 :int16 datatype item
                                             ary-data offset n-elems)
      [:char :char] (java-array-buffer->io :char :char datatype item
                                           ary-data offset n-elems)
      [:int32 :uint32] (java-array-buffer->io :int32 :uint32 datatype item
                                              ary-data offset n-elems)
      [:int32 :int32] (java-array-buffer->io :int32 :int32 datatype item
                                             ary-data offset n-elems)
      [:int64 :uint64] (java-array-buffer->io :int64 :uint64 datatype item
                                              ary-data offset n-elems)
      [:int64 :int64] (java-array-buffer->io :int64 :int64 datatype item
                                             ary-data offset n-elems)
      [:float32 :float32] (java-array-buffer->io :float32 :float32 datatype item
                                                 ary-data offset n-elems)
      [:float64 :float64] (java-array-buffer->io :float64 :float64 datatype item
                                                 ary-data offset n-elems)
      (java-array-buffer->io :object :object datatype item ary-data offset n-elems))))


(defn array-buffer
  ([java-ary]
   (ArrayBuffer. java-ary 0 (count java-ary)
                 (dtype-proto/elemwise-datatype java-ary)
                 {}
                 nil))
  ([java-ary buf-dtype]
   (ArrayBuffer. java-ary 0 (count java-ary) buf-dtype {}
                 nil)))


(defn array-buffer->map
  "Convert an array buffer to a map of
  {:java-array :offset :length :datatype}"
  [^ArrayBuffer ary-buf]
  {:java-array (.ary-data ary-buf)
   :offset (.offset ary-buf)
   :length (.n-elems ary-buf)
   :datatype (.datatype ary-buf)
   :metadata (.metadata ary-buf)})


(defn is-array-type?
  [item]
  (when item
    (.isArray (.getClass ^Object item))))


(def array-types
  (set (concat casting/host-numeric-types
               [:boolean :object :char])))


(defn- as-object ^Object [item] item)


(defmacro initial-implement-arrays
  []
  `(do
     ~@(->>
        array-types
        (map
         (fn [ary-type]
           (let [ary-dtype (keyword (str (name ary-type) "-array"))]
             `(do
                (casting/add-object-datatype! ~ary-dtype ~(typecast/datatype->array-cls ary-type)
                                              false)
                (extend-type ~(typecast/datatype->array-cls ary-type)
                  dtype-proto/PElemwiseDatatype
                  (elemwise-datatype [~'item]
                    ~(if (not= ary-type :object)
                       `~ary-type
                       `(-> (as-object ~'item)
                            (.getClass)
                            (.getComponentType)
                            (casting/object-class->datatype))))
                  dtype-proto/PDatatype
                  (datatype [item#] ~ary-dtype)
                  dtype-proto/PECount
                  (ecount [item#]
                    (alength
                     (typecast/datatype->array ~ary-type item#)))
                  ;;Java stores array data in memory as big-endian.  Holdover
                  ;;from Sun Spark architecture.
                  dtype-proto/PEndianness
                  (endianness [item#] :big-endian)
                  dtype-proto/PToArrayBuffer
                  (convertible-to-array-buffer? [item#] true)
                  (->array-buffer [item#]
                    (ArrayBuffer. item# 0
                                  (alength (typecast/datatype->array ~ary-type
                                                                     item#))
                                  (dtype-proto/elemwise-datatype item#)
                                  {}
                                  nil))
                  dtype-proto/PSubBuffer
                  (sub-buffer [item# off# len#]
                    (ArrayBuffer. item#
                                  (int off#)
                                  (int len#)
                                  (dtype-proto/elemwise-datatype item#)
                                  {}
                                  nil))
                  dtype-proto/PToReader
                  (convertible-to-reader? [item#] true)
                  (->reader [item#]
                    (dtype-proto/->reader (array-buffer item#)))
                  dtype-proto/PToWriter
                  (convertible-to-writer? [item#] true)
                  (->writer [item#]
                    (dtype-proto/->writer (array-buffer item#)))))))))))


(initial-implement-arrays)


(deftype ByteArrayBinaryBufferLE [^bytes ary-data
                                  ^Buffer buffer
                                  ^long offset
                                  ^long n-elems]
  dtype-proto/PEndianness
  (endianness [this] :little-endian)
  BinaryBuffer
  (lsize [this] n-elems)
  (allowsBinaryRead [this] true)
  (readBinByte [this byteOffset] (aget ary-data (+ offset byteOffset)))
  (readBinShort [this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/shortFromBytesLE (aget ary-data byteOffset)
                                        (aget ary-data (+ byteOffset 1)))))
  (readBinInt [this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/intFromBytesLE (aget ary-data byteOffset)
                                      (aget ary-data (+ byteOffset 1))
                                      (aget ary-data (+ byteOffset 2))
                                      (aget ary-data (+ byteOffset 3)))))
  (readBinLong [this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/longFromBytesLE (aget ary-data byteOffset)
                                       (aget ary-data (+ byteOffset 1))
                                       (aget ary-data (+ byteOffset 2))
                                       (aget ary-data (+ byteOffset 3))
                                       (aget ary-data (+ byteOffset 4))
                                       (aget ary-data (+ byteOffset 5))
                                       (aget ary-data (+ byteOffset 6))
                                       (aget ary-data (+ byteOffset 7)))))
  (readBinFloat [this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/floatFromBytesLE (aget ary-data byteOffset)
                                        (aget ary-data (+ byteOffset 1))
                                        (aget ary-data (+ byteOffset 2))
                                        (aget ary-data (+ byteOffset 3)))))
  (readBinDouble [this byteOffset]
    (let [byteOffset (+ byteOffset offset)]
      (ByteConversions/doubleFromBytesLE (aget ary-data byteOffset)
                                         (aget ary-data (+ byteOffset 1))
                                         (aget ary-data (+ byteOffset 2))
                                         (aget ary-data (+ byteOffset 3))
                                         (aget ary-data (+ byteOffset 4))
                                         (aget ary-data (+ byteOffset 5))
                                         (aget ary-data (+ byteOffset 6))
                                         (aget ary-data (+ byteOffset 7)))))

  (allowsBinaryWrite [this] true)
  (writeBinByte [this byteOffset data]
    (aset ary-data (+ byteOffset offset) data))
  (writeBinShort [this byteOffset data]
    (ByteConversions/shortToWriterLE data buffer (+ byteOffset offset)))
  (writeBinInt [this byteOffset data]
    (ByteConversions/intToWriterLE data buffer (+ byteOffset offset)))
  (writeBinLong [this byteOffset data]
    (ByteConversions/longToWriterLE data buffer (+ byteOffset offset)))
  (writeBinFloat [this byteOffset data]
    (ByteConversions/floatToWriterLE data buffer (+ byteOffset offset)))
  (writeBinDouble [this byteOffset data]
    (ByteConversions/doubleToWriterLE data buffer (+ byteOffset offset)))
  dtype-proto/PClone
  (clone [this]
    (-> (dtype-proto/sub-buffer ary-data offset n-elems)
        (dtype-proto/clone)
        (dtype-proto/->binary-buffer)))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [this] true)
  (->array-buffer [this]
    (-> (dtype-proto/->array-buffer ary-data)
        (dtype-proto/sub-buffer offset n-elems)))
  dtype-proto/PSubBuffer
  (sub-buffer [this off len]
    (let [off (long off)
          len (long len)]
      (ByteArrayBinaryBufferLE. ary-data
                                (-> (dtype-proto/->array-buffer this)
                                    (dtype-proto/sub-buffer off len)
                                    (dtype-proto/->buffer))
                                (+ offset off)
                                len))))


(extend-type (Class/forName "[B")
  dtype-proto/PToBinaryBuffer
  (convertible-to-binary-buffer? [ary] true)
  (->binary-buffer [ary]
    (ByteArrayBinaryBufferLE. ary
                              (dtype-proto/->buffer ary)
                              0 (alength ^bytes ary))))
