(ns tech.v3.tensor-api
  "ND bindings for the tech.v3.datatype system.  A Tensor is conceptually just a tuple
  of a buffer and an index operator that is capable of converting indexes in ND space
  into a single long index into the buffer.  Tensors implement the
  tech.v3.datatype.NDBuffer interface and outside this file ND objects are expected to
  simply implement that interface.

  This system relies heavily on the tech.v3.tensor.dimensions namespace to provide the
  optimized indexing operator from ND space to buffer space and back.

  There is an ABI in the form of nd-buffer-descriptors that is a map containing:

  * `:ptr` - long value
  * `:elemwise-datatype` - primitive datatype of the buffer.
  * `:shape` - buffer of `:int64` dimensions.
  * `:strides` - buffer of `:int64` byte stride counts.

  Optionally more keys and the source agrees not to release the source data until
  this map goes out of scope."
  (:require [tech.v3.datatype.base :as dtype-base]
            [tech.v3.datatype.errors :as errors]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.native-buffer :as native-buffer]
            [tech.v3.datatype.io-indexed-buffer :as indexed-buffer]
            [tech.v3.datatype.const-reader :refer [const-reader]]
            [tech.v3.datatype.protocols :as dtype-proto]
            [tech.v3.datatype.copy-make-container :as dtype-cmc]
            [tech.v3.datatype.pprint :as dtype-pp]
            [tech.v3.datatype.argops :as argops]
            [tech.v3.datatype.argtypes :refer [arg-type]]
            [tech.v3.datatype.emap :as emap]
            [tech.v3.tensor.pprint :as tens-pp]
            [tech.v3.tensor.dimensions :as dims]
            [tech.v3.tensor.dimensions.analytics :as dims-analytics]
            [tech.v3.tensor.dimensions.shape :as dims-shape]
            [tech.v3.tensor.tensor-copy :as tens-cpy]
            [tech.v3.datatype.export-symbols :as export-symbols]
            [tech.v3.parallel.for :as parallel-for]
            [com.github.ztellman.primitive-math :as pmath]
            [clojure.tools.logging :as log]
            [ham-fisted.api :as hamf])
  (:import [clojure.lang IObj IFn$OLO IFn$ODO IFn
            IFn$LOO IFn$LLOO IFn$LLLOO]
           [tech.v3.datatype LongNDReader Buffer NDBuffer
            ObjectReader LongReader NDReduce]
           [java.util List]
           [ham_fisted ChunkedList]))


(set! *warn-on-reflection* true)
(set! *unchecked-math* :warn-on-boxed)

(declare construct-tensor tensor-copy!)



(extend-type NDBuffer
  dtype-proto/PElemwiseCast
  (elemwise-cast [t new-dtype]
    (construct-tensor (dtype-proto/elemwise-cast
                       (or (.buffer t)
                           (.bufferIO t)) new-dtype)
                      (.dimensions t)
                      (meta t)))
  dtype-proto/PElemwiseReaderCast
  (elemwise-reader-cast [t new-dtype]
    (let [new-tens
          (construct-tensor (dtype-proto/elemwise-reader-cast
                             (or (.buffer t)
                                 (.bufferIO t)) new-dtype)
                            (.dimensions t)
                            (meta t))]
      (.bufferIO ^NDBuffer new-tens)))
  dtype-proto/PDatatype
  (datatype [this] :tensor)
  dtype-proto/PShape
  (shape [t] (.shape t))
  dtype-proto/PClone
  (clone [t]
    (tensor-copy! t
                  (construct-tensor
                   (dtype-cmc/make-container (dtype-proto/elemwise-datatype t)
                                             (dtype-base/ecount t))
                   (dims/dimensions (dtype-proto/shape t))
                   (meta t))))
  dtype-proto/PToNDBufferDesc
  (convertible-to-nd-buffer-desc? [item]
    (and (.buffer item)
         (dtype-proto/convertible-to-native-buffer? (.buffer item))
         (dims/direct? (.dimensions item))))
  (->nd-buffer-descriptor [item]
    (let [item-buf (.buffer item)
          nbuf (dtype-base/->native-buffer (.buffer item))]
      {:ptr (.address nbuf)
       :datatype :tensor
       :elemwise-datatype (dtype-base/elemwise-datatype item-buf)
       :endianness (dtype-proto/endianness nbuf)
       :shape (dtype-base/shape item)
       :strides (mapv (partial * (casting/numeric-byte-width
                                  (dtype-base/elemwise-datatype item-buf)))
                      (:strides (.dimensions item)))
       ;;Include the native buffer so gc references are kept.
       :native-buffer nbuf}))
  dtype-proto/PToBuffer
  (convertible-to-buffer? [t] true)
  (->buffer [t] (.bufferIO t))
  dtype-proto/PToReader
  (convertible-to-reader? [t] (.allowsRead t))
  (->reader [t] (dtype-proto/->buffer t))
  dtype-proto/PToWriter
  (convertible-to-writer? [t] (.allowsWrite t))
  (->writer [t] (dtype-proto/->buffer t))
  dtype-proto/PToArrayBuffer
  (convertible-to-array-buffer? [t]
    (when-let [buffer (.buffer t)]
      (and (dtype-proto/convertible-to-array-buffer? buffer)
           (dims/native? (.dimensions t)))))
  (->array-buffer [t] (dtype-proto/->array-buffer (.buffer t)))
  dtype-proto/PToNativeBuffer
  (convertible-to-native-buffer? [t]
    (when-let [buffer (.buffer t)]
      (and  (dtype-proto/convertible-to-native-buffer? buffer)
            (dims/native? (.dimensions t)))))
  (->native-buffer [t] (dtype-proto/->native-buffer (.buffer t)))
  dtype-proto/PApplyUnary
  (apply-unary-op [t res-dtype un-op]
    (construct-tensor
     (dtype-proto/apply-unary-op (or (.buffer t) (.bufferIO t)) res-dtype un-op)
     (.dimensions t)
     (meta t)))
  dtype-proto/PTensor
  (reshape [t new-shape]
    (if (= (dtype-proto/shape t) new-shape)
      t
      (let [n-elems (long (apply * new-shape))]
        (construct-tensor
         (dtype-proto/sub-buffer (.bufferIO t) 0 n-elems)
         (dims/dimensions new-shape)
         (meta t)))))
  (select [t select-args]
    (let [{buf-offset :elem-offset
           buf-len :buffer-ecount
           :as new-dims}
          (apply dims/select (.dimensions t) select-args)
          buffer (or (.buffer t) (.bufferIO t))
          buf-offset (long buf-offset)
          new-buffer (if-not (and (== buf-offset 0)
                                  (or (not buf-len)
                                      (== (dtype-base/ecount buffer) (long buf-len))))
                       (if buf-len
                         (dtype-base/sub-buffer buffer buf-offset buf-len)
                         (dtype-base/sub-buffer buffer buf-offset))
                       buffer)]
    (construct-tensor new-buffer new-dims (meta t))))
  (transpose [t transpose-vec]
    (construct-tensor (or (.buffer t)
                          (.bufferIO t))
                      (dims/transpose (.dimensions t) transpose-vec)
                      (meta t)))
  (broadcast [t bcast-shape]
    (let [tens-shape (dtype-base/shape t)
          n-tens-elems (dtype-base/ecount t)
          n-bcast-elems (dims-shape/ecount bcast-shape)
          num-tens-shape (count tens-shape)]
      (when-not (every? number? bcast-shape)
        (throw (ex-info "Broadcast shapes must only be numbers" {})))
      (when-not (>= n-bcast-elems
                    n-tens-elems)
        (throw (ex-info
                (format "Improper broadcast shape (%s), smaller than tens (%s)"
                        bcast-shape tens-shape)
                {})))
      (when-not (every? (fn [[item-dim bcast-dim]]
                          (= 0 (rem (int bcast-dim)
                                    (int item-dim))))
                        (map vector tens-shape (take-last num-tens-shape bcast-shape)))
        (throw (ex-info
                (format "Broadcast shape (%s) is not commensurate with tensor shape %s"
                        bcast-shape tens-shape)
                {})))
      (construct-tensor (or (.buffer t)
                            (.bufferIO t))
                        (dims/broadcast (.dimensions t)
                                        bcast-shape)
                        (meta t))))
  (rotate [t offset-vec]
    (construct-tensor (or (.buffer t) (.bufferIO t))
                      (dims/rotate (.dimensions t)
                                   (mapv #(* -1 (long %)) offset-vec))
                      (meta t)))
  (slice [tens slice-dims right?]
    (let [t-shape (dtype-base/shape tens)
          n-shape (count t-shape)
          slice-dims (long slice-dims)
          dimensions (.dimensions tens)]
      (when-not (<= slice-dims n-shape)
        (throw (ex-info (format "Slice operator n-dims out of range: %s:%s"
                                slice-dims t-shape)
                        {})))
      (if (== slice-dims n-shape)
        (dtype-base/->reader tens)
        (let [{:keys [dimensions offsets]}
              (if right?
                (dims/slice-right dimensions slice-dims)
                (dims/slice dimensions slice-dims))
              ^Buffer offsets (dtype-base/->reader offsets)
              n-offsets (.lsize offsets)
              tens-buf (or (.buffer tens) (.bufferIO tens))
              buf-ecount (:buffer-ecount dimensions)]
          (reify ObjectReader
            (lsize [rdr] n-offsets)
            (readObject [rdr idx]
              (construct-tensor (dtype-base/sub-buffer
                                 tens-buf
                                 (.readLong offsets idx)
                                 buf-ecount)
                                dimensions)))))))
  (mget [t idx-seq]
    (.ndReadObjectIter t idx-seq))
  (mset! [t idx-seq value]
    (.ndWriteObjectIter t idx-seq value)))


(dtype-pp/implement-tostring-print NDBuffer)


(deftype ^:private DataTensor [buffer dimensions
                               ^long rank
                               ^LongNDReader index-system
                               ^Buffer cached-io
                               metadata]
  dtype-proto/PECount
  (ecount [t] (.lsize t))
  NDBuffer
  (lsize [_t] (.lsize index-system))
  (elemwiseDatatype [_t] (dtype-proto/elemwise-datatype buffer))
  (buffer [_t] buffer)
  (bufferIO [t]
    (if (dims/native? dimensions)
      (dtype-proto/->buffer buffer)
      (indexed-buffer/indexed-buffer (.indexSystem t) buffer)))
  (dimensions [_t] dimensions)
  (indexSystem [_t] index-system)
  (ndReadLong [_t idx]
    (.readLong cached-io (.ndReadLong index-system idx)))
  (ndReadLong [_t row col]
    (.readLong cached-io (.ndReadLong index-system row col)))
  (ndReadLong [_t height width chan]
    (.readLong cached-io (.ndReadLong index-system height width chan)))
  (ndWriteLong [_t idx value]
    (.writeLong cached-io (.ndReadLong index-system idx) value))
  (ndWriteLong [_t row col value]
    (.writeLong cached-io (.ndReadLong index-system row col) value))
  (ndWriteLong [_t height width chan value]
    (.writeLong cached-io (.ndReadLong index-system height width chan)
                value))

  (ndReadDouble [_t idx]
    (.readDouble cached-io (.ndReadLong index-system idx)))
  (ndReadDouble [_t row col]
    (.readDouble cached-io (.ndReadLong index-system row col)))
  (ndReadDouble [_t height width chan]
    (.readDouble cached-io (.ndReadLong index-system height width chan)))
  (ndWriteDouble [_t idx value]
    (.writeDouble cached-io (.ndReadLong index-system idx) value))
  (ndWriteDouble [_t row col value]
    (.writeDouble cached-io (.ndReadLong index-system row col) value))
  (ndWriteDouble [_t height width chan value]
    (.writeDouble cached-io (.ndReadLong index-system height width chan)
                  value))

  (ndReadObject [t idx]
    (if (== 1 rank)
      (.readObject cached-io (.ndReadLong index-system idx))
      (dtype-proto/select t [idx])))
  (ndReadObject [t row col]
    (if (== 2 rank)
      (.readObject cached-io (.ndReadLong index-system row col))
      (dtype-proto/select t [row col])))
  (ndReadObject [t height width chan]
    (if (== 3 rank)
      (.readObject cached-io (.ndReadLong index-system height width chan))
      (dtype-proto/select t [height width chan])))
  (ndReadObjectIter [t indexes]
    (if (== (count indexes) rank)
      (.readObject cached-io (.ndReadLongIter index-system indexes))
      (dtype-proto/select t indexes)))
  (ndWriteObject [t idx value]
    (if (== 1 rank)
      (.writeObject cached-io (.ndReadLong index-system idx) value)
      (tensor-copy! value (.ndReadObject t idx))))
  (ndWriteObject [t row col value]
    (if (== 2 rank)
      (.writeObject cached-io (.ndReadLong index-system row col) value)
      (tensor-copy! value (.ndReadObject t row col))))
  (ndWriteObject [t height width chan value]
    (if (== 3 rank)
      (.writeObject cached-io (.ndReadLong index-system height width chan)
                    value)
      (tensor-copy! value (.ndReadObject t height width chan))))
  (ndWriteObjectIter [t indexes value]
    (if (== (count indexes) rank)
      (.writeObject cached-io (.ndReadLongIter index-system indexes) value)
      (tensor-copy! value (dtype-proto/select t indexes))))

   (ndAccumPlusLong [_t c value]
     (.accumPlusLong cached-io (.ndReadLong index-system c) value))
   (ndAccumPlusLong [_t x c value]
     (.accumPlusLong cached-io (.ndReadLong index-system x c) value))
   (ndAccumPlusLong [_t y x c value]
     (.accumPlusLong cached-io (.ndReadLong index-system y x c) value))

   (ndAccumPlusDouble [_t c value]
     (.accumPlusDouble cached-io (.ndReadLong index-system c) value))
   (ndAccumPlusDouble [_t x c value]
     (.accumPlusDouble cached-io (.ndReadLong index-system x c) value))
   (ndAccumPlusDouble [_t y x c value]
     (.accumPlusDouble cached-io (.ndReadLong index-system y x c) value))

  (allowsRead [_t] (.allowsRead cached-io))
  (allowsWrite [_t] (.allowsWrite cached-io))
  (iterator [t]
    (.iterator (dtype-proto/slice t 1 false)))
  IObj
  (meta [_item] metadata)
  (withMeta [_item metadata]
    (DataTensor. buffer dimensions rank index-system cached-io metadata))
  Object
  (toString [t] (tens-pp/tensor->string t)))


;; Override the autogenerated functions
(defn- ->DataTensor [& args] (throw (Exception. "Please use ->tensor")))

(dtype-pp/implement-tostring-print DataTensor)


(casting/add-object-datatype! :tensor NDBuffer false)


;;Tensor used with native dimensions; ones with in-order strides
(deftype ^:private DirectTensor [buffer dimensions
                                 ^long rank
                                 ^LongNDReader index-system
                                 ^Buffer cached-io
                                 ^long y
                                 ^long x
                                 ^long c
                                 metadata]
  dtype-proto/PECount
  (ecount [t] (.lsize t))
  NDBuffer
  (elemwiseDatatype [_t] (dtype-proto/elemwise-datatype buffer))
  (buffer [_t] buffer)
  (bufferIO [t]
    (if (dims/native? dimensions)
      (dtype-proto/->buffer buffer)
      (indexed-buffer/indexed-buffer (.indexSystem t) buffer)))
  (dimensions [_t] dimensions)
  (indexSystem [_t] index-system)
  (lsize [_t] (.lsize index-system))

  (ndReadLong [_t idx]
    (.readLong cached-io (* idx c)))
  (ndReadLong [_t row col]
    (.readLong cached-io (+ (* row x) (* col c))))
  (ndReadLong [_t height width chan]
    (.readLong cached-io (+ (* height y) (* width x) (* chan c))))
  (ndWriteLong [_t idx value]
    (.writeLong cached-io (* idx c) value))
  (ndWriteLong [_t row col value]
    (.writeLong cached-io (+ (* row x) (* col c)) value))
  (ndWriteLong [_t height width chan value]
    (.writeLong cached-io (+ (* height y) (* width x) (* chan c))
                value))

  (ndReadDouble [_t idx]
    (.readDouble cached-io (* idx c)))
  (ndReadDouble [_t row col]
    (.readDouble cached-io (+ (* row x) (* col c))))
  (ndReadDouble [_t height width chan]
    (.readDouble cached-io (+ (* height y) (* width x) (* chan c))))
  (ndWriteDouble [_t idx value]
    (.writeDouble cached-io (* idx c) value))
  (ndWriteDouble [_t row col value]
    (.writeDouble cached-io (+ (* row x) (* col c)) value))
  (ndWriteDouble [_t height width chan value]
    (.writeDouble cached-io (+ (* height y) (* width x) (* chan c))
                  value))

  (ndReadObject [t idx]
    (if (== 1 rank)
      (.readObject cached-io (* idx c))
      (dtype-proto/select t [idx])))
  (ndReadObject [t row col]
    (if (== 2 rank)
      (.readObject cached-io (+ (* row x) (* col c)))
      (dtype-proto/select t [row col])))
  (ndReadObject [t height width chan]
    (if (== 3 rank)
      (.readObject cached-io (+ (* height y) (* width x) (* chan c)))
      (dtype-proto/select t [height width chan])))
  (ndReadObjectIter [t indexes]
    (if (== (count indexes) rank)
      (.readObject cached-io (.ndReadLongIter index-system indexes))
      (dtype-proto/select t indexes)))
  (ndWriteObject [t idx value]
    (if (== 1 rank)
      (.writeObject cached-io (* idx c) value)
      (tensor-copy! value (.ndReadObject t idx))))
  (ndWriteObject [t row col value]
    (if (== 2 rank)
      (.writeObject cached-io (+ (* row x) (* col c)) value)
      (tensor-copy! value (.ndReadObject t row col))))
  (ndWriteObject [t height width chan value]
    (if (== 3 rank)
      (.writeObject cached-io (+ (* height y) (* width x) (* chan c))
                    value)
      (tensor-copy! value (.ndReadObject t height width chan))))
  (ndWriteObjectIter [t indexes value]
    (if (== (count indexes) rank)
      (.writeObject cached-io (.ndReadLongIter index-system indexes) value)
      (tensor-copy! value (dtype-proto/select t indexes))))

   (ndAccumPlusLong [_t idx value]
     (.accumPlusLong cached-io (* idx c) value))
   (ndAccumPlusLong [_t row chan value]
     (.accumPlusLong cached-io (+ (* row x) (* chan c)) value))
   (ndAccumPlusLong [_t height width chan value]
     (.accumPlusLong cached-io (+ (* height y) (* width x) (* chan c)) value))

   (ndAccumPlusDouble [_t idx value]
     (.accumPlusDouble cached-io (* idx c) value))
   (ndAccumPlusDouble [_t width chan value]
     (.accumPlusDouble cached-io (+ (* width x) (* chan c)) value))
   (ndAccumPlusDouble [_t height width chan value]
     (.accumPlusDouble cached-io (+ (* height y) (* width x) (* chan c)) value))

  (allowsRead [_t] (.allowsRead cached-io))
  (allowsWrite [_t] (.allowsWrite cached-io))
  (iterator [t]
    (.iterator (dtype-proto/slice t 1 false)))
  IObj
  (meta [_item] metadata)
  (withMeta [_item metadata]
    (DataTensor. buffer dimensions rank index-system cached-io metadata))
  Object
  (toString [t] (tens-pp/tensor->string t)))


(defn- ->DirectTensor [& args] (throw (Exception. "Please use ->tensor")))


(dtype-pp/implement-tostring-print DirectTensor)


(defn construct-tensor
  "Construct an implementation of tech.v3.datatype.NDBuffer from a buffer and
  a dimensions object.  See dimensions/dimensions."
  ^NDBuffer [buffer dimensions & [metadata]]
  (try
    (let [nd-desc  (dims/->global->local dimensions)]
      (if (dims/direct? dimensions)
        (let [strides (:strides dimensions)
              n-strides (count strides)
              y (if (>= n-strides 3)
                  (nth strides 0)
                  0)
              x (if (>= n-strides 2)
                  (nth strides (- n-strides 2))
                  0)
              c (last strides)]
          (DirectTensor. buffer dimensions
                         (.rank nd-desc)
                         nd-desc
                         (if (dtype-proto/convertible-to-buffer? buffer)
                           (dtype-proto/->buffer buffer)
                           (dtype-base/->reader buffer))
                         y x c
                         metadata))
        (DataTensor. buffer dimensions
                     (.rank nd-desc)
                     nd-desc
                     (if (dtype-proto/convertible-to-buffer? buffer)
                       (dtype-proto/->buffer buffer)
                       (dtype-base/->reader buffer))
                     metadata)))
    (catch Throwable e
      (log/errorf "Failed to produce tensor for dimensions %s, (reduced) %s"
                  (pr-str (select-keys dimensions [:shape :strides]))
                  (pr-str (dims-analytics/reduce-dimensionality dimensions)))
      (throw e))))


(defn tensor?
  "Returns true if this implements the tech.v3.datatype.NDBuffer interface."
  [item]
  (instance? NDBuffer item))


(defn tensor->buffer
  "Get the buffer from a tensor."
  [item]
  (errors/when-not-error (instance? NDBuffer item)
    "Item is not a tensor")
  (.buffer ^NDBuffer item))


(defn tensor->dimensions
  "Get the dimensions object from a tensor."
  [item]
  (errors/when-not-error (instance? NDBuffer item)
    "Item is not a tensor")
  (.dimensions ^NDBuffer item))


(defn simple-dimensions?
  "Are the dimensions of this object simple meaning read in order with no
  breaks due to striding."
  [item]
  (dims/native? (tensor->dimensions item)))


(defn dims-suitable-for-desc?
    "Are the dimensions of this object suitable for use in a buffer description?
  breaks due to striding."
  [item]
  (dims/direct? (tensor->dimensions item)))


(defn ->tensor
  "Convert some data into a tensor via copying the data.  The datatype and container
  type can be specified.  The datatype defaults to the datatype of the input data and container
  type defaults to jvm-heap.

  Options:

  * `:datatype` - Data of the storage.  Defaults to the datatype of the passed-in data.
  * `:container-type` - Specify the container type of the new tensor.  Defaults to
    `:jvm-heap`.
  * `:resource-type` - One of `tech.v3.resource/track` `:track-type` options.  If allocating
     native tensors, `nil` corresponds to `:gc:`."
  ^NDBuffer [data & {:keys [datatype container-type]
                     :as options}]
  (let [data-shape (dtype-base/shape data)
        datatype (if (dtype-base/array? data)
                   (dtype-base/nested-array-elemwise-datatype data)
                   (or datatype (dtype-base/elemwise-datatype data)))
        container-type (or container-type :jvm-heap)
        n-elems (apply * 1 data-shape)]
    (construct-tensor
     (first
      (dtype-proto/copy-raw->item!
       data
       (dtype-cmc/make-container container-type datatype
                                 (assoc options :uninitialized? true)
                                 n-elems)
       0 (assoc options :rectangular? true)))
     (dims/dimensions data-shape))))


(defn as-tensor
  "Attempts an in-place conversion of this object to a tech.v3.datatype.NDBuffer interface.
  For a guaranteed conversion, use ensure-tensor."
  ^NDBuffer [data]
  (if (instance? NDBuffer data)
    data
    (dtype-proto/as-tensor data)))


(defn new-tensor
  "Create a new tensor with a given shape.

  Options:

  * `:datatype` - Data of the storage.  Defaults to `:float64`.
  * `:container-type` - Specify the container type of the new tensor.  Defaults to
    `:jvm-heap`.
  * `:resource-type` - One of `tech.v3.resource/track` `:track-type` options.  If allocating
     native tensors, `nil` corresponds to `:gc:`."
  ^NDBuffer [shape & {:keys [datatype container-type]
            :as options}]
  (let [datatype (or datatype :float64)
        container-type (or container-type :jvm-heap)
        n-elems (apply * 1 shape)]
    (construct-tensor
     (dtype-cmc/make-container container-type datatype options n-elems)
     (dims/dimensions shape))))


(defn const-tensor
  "Construct a tensor from a value and a shape.  Data is represented efficiently via a const-reader."
  ^NDBuffer [value shape]
  (let [dims (dims/dimensions shape)
        n-elems (dims/ecount dims)]
    (construct-tensor (const-reader value n-elems) dims)))


(defn ensure-tensor
  "Create an implementation of tech.v3.datatype.NDBuffer from an
  object.  If possible, represent the data in-place."
  ^NDBuffer [item]
  (if (instance? NDBuffer item)
    item
    (if-let [item (dtype-proto/as-tensor item)]
      item
      (cond
        (dtype-base/as-concrete-buffer item)
        (construct-tensor (dtype-base/as-concrete-buffer item)
                          (dims/dimensions (dtype-base/shape item)))
        (and (dtype-proto/convertible-to-reader? item)
             (= 1 (count (dtype-base/shape item))))
        (construct-tensor item (dims/dimensions (dtype-base/shape item)))
        :else
        (->tensor item)))))


;;Defaults for tensor protocols
(extend-type Object
  dtype-proto/PTensor
  (reshape [t new-shape]
    (-> (ensure-tensor t)
        (dtype-proto/reshape new-shape)))
  (select [t select-args]
    (-> (ensure-tensor t)
        (dtype-proto/select select-args)))
  (transpose [t reorder-vec]
    (-> (ensure-tensor t)
        (dtype-proto/transpose reorder-vec)))
  (broadcast [t new-shape]
    (-> (ensure-tensor t)
        (dtype-proto/broadcast new-shape)))
  (rotate [t offset-vec]
    (-> (ensure-tensor t)
        (dtype-proto/rotate offset-vec)))
  (slice [t n-dims right?]
    (-> (ensure-tensor t)
        (dtype-proto/slice n-dims right?)))
  (mget [t idx-seq]
    (errors/when-not-error
     (== 1 (count idx-seq))
     "Generic mget on reader can only have 1 dimension")
    (dtype-base/get-value t (first idx-seq)))
  (mset! [t idx-seq value]
    (errors/when-not-error
     (== 1 (count idx-seq))
     "Generic mset on reader can only have 1 dimension")
    (dtype-base/set-value! t (first idx-seq) value))
  dtype-proto/PToTensor
  (as-tensor [item] nil))


(defn tensor-copy!
  "Specialized copy with optimized pathways for when tensors have regions of contiguous
  data.  As an example consider a sub-image of a larger image.  Each row can be copied
  contiguously into a new image but there are gaps between them."
  ([src dst options]
   (let [src-argtype (arg-type src)
         src (if (= src-argtype :scalar)
               (const-tensor src (dtype-base/shape dst))
               (ensure-tensor src))]
     (tens-cpy/tensor-copy! src dst options)))
  ([src dst]
   (tensor-copy! src dst nil)))


(defn dimensions-dense?
  "Returns true of the dimensions of a tensor are dense, meaning no gaps due to
  striding."
  [^NDBuffer tens]
  (dims/dense? (.dimensions tens)))


(defn rows
  "Return the rows of the tensor in a randomly-addressable structure."
  ^List [^NDBuffer src]
  (errors/when-not-error (>= (.rank src) 2)
    "Tensor has too few dimensions")
  (dtype-base/slice src 1))


(defn columns
  "Return the columns of the tensor in a randomly-addressable structure."
  ^List [^NDBuffer src]
  (errors/when-not-error (>= (.rank src) 2)
    "Tensor has too few dimensions")
  (dtype-base/slice-right src (dec (.rank src))))


(defn clone
  "Clone a tensor via copying the tensor into a new container.  Datatype defaults
  to the datatype of the tensor and container-type defaults to `:java-heap`.

  Options:

  * `:datatype` - Specify a new datatype to copy data into.
  * `:container-type` - Specify the container type of the new tensor.
     Defaults to `:jvm-heap`.
    * `:resource-type` - One of `tech.v3.resource/track` `:track-type` options.  If allocating
     native tensors, `nil` corresponds to `gc:`."
  ^NDBuffer [tens & {:keys [datatype]
                     :or {datatype (dtype-base/elemwise-datatype tens)}
                     :as options}]
  (dtype-cmc/copy! tens (apply new-tensor (dtype-base/shape tens)
                               (->> (assoc options :datatype datatype)
                                    (seq)
                                    (apply concat)))))


(defn ->jvm
  "Conversion to storage that is efficient for the jvm.
  Base storage is either jvm-array or persistent-vector."
  [item & {:keys [datatype base-storage]
           :or {base-storage :persistent-vector}}]
  ;;Get the data off the device
  (let [item-shape (dtype-base/shape item)
        item-ecount (dtype-base/ecount item)
        column-len (long (last item-shape))
        n-columns (quot item-ecount column-len)
        datatype (or datatype (dtype-base/elemwise-datatype item))
        data-array (dtype-proto/->reader item)
        base-data
        (->> (range n-columns)
             (map (fn [col-idx]
                    (let [col-offset (* column-len (long col-idx))]
                      (case base-storage
                        :java-array
                        (let [retval (->
                                      (dtype-cmc/make-container datatype column-len)
                                      (dtype-cmc/->array))]
                          (dtype-cmc/copy! (dtype-base/sub-buffer
                                            data-array col-offset column-len)
                                           retval))
                        :persistent-vector
                        (->> (dtype-base/sub-buffer data-array col-offset column-len)
                             (dtype-base/->reader)
                             (vec)))))))
        partitionv (fn [& args]
                     (map vec (apply partition args)))
        partition-shape (->> (rest item-shape)
                             drop-last
                             reverse)]
    (if (> (count item-shape) 1)
      (->> partition-shape
           (reduce (fn [retval part-value]
                     (partitionv part-value retval))
                   base-data)
           vec)
      (first base-data))))


(defn ensure-nd-buffer-descriptor
  "Get a buffer descriptor from the tensor.  This may copy the data.  If you want to
  ensure sharing, use the protocol ->nd-buffer-descriptor function."
  [tens]
  (let [tens (ensure-tensor tens)]
    (if (dtype-proto/convertible-to-nd-buffer-desc? tens)
      (dtype-proto/->nd-buffer-descriptor tens)
      (-> (clone tens :container-type :native-heap)
          dtype-proto/->nd-buffer-descriptor))))


(defn nd-buffer-descriptor->tensor
  "Given a buffer descriptor, produce a tensor"
  [{:keys [ptr elemwise-datatype shape strides] :as desc}]
  (when (or (not ptr)
            (= 0 (long ptr)))
    (throw (ex-info "Cannot create tensor from nil pointer."
                    {:ptr ptr})))
  (let [dtype-size (casting/numeric-byte-width elemwise-datatype)]
    (when-not (every? #(= 0 (rem (long %)
                                 dtype-size))
                      strides)
      (throw (ex-info "Strides are not commensurate with datatype size." {})))
    (let [max-stride-idx (argops/argmax strides)
          buffer-len (* (long (dtype-base/get-value shape max-stride-idx))
                        (long (dtype-base/get-value strides max-stride-idx)))
          ;;Move strides into elem-count instead of byte-count
          strides (mapv #(quot (long %) dtype-size)
                        strides)]
      (-> (native-buffer/wrap-address ptr buffer-len elemwise-datatype
                                      (dtype-proto/platform-endianness)
                                      desc)
          (construct-tensor (dims/dimensions shape strides))))))


(export-symbols/export-symbols tech.v3.datatype.base
                               reshape
                               select
                               transpose
                               broadcast
                               rotate
                               slice
                               slice-right
                               mget
                               mset!)


(defn- shape-stride-reader
  [^longs shape ^Buffer strides ^long global-idx]
  (let [n-dims (alength shape)]
    (reify LongReader
      (lsize [rdr] n-dims)
      (readLong [rdr idx]
        (-> (quot global-idx (.readLong strides idx))
            (rem (aget shape idx)))))))

(defn- round
  ^long [^long amount ^long div]
  (let [res (rem amount div)]
    (- amount (rem amount div))))


(defn- nd-reduce
  [^NDBuffer buffer rfn acc sidx eidx]
  (let [sidx (long sidx)
        eidx (long eidx)
        buf-shape (.shape buffer)
        n-dims (count buf-shape)]

    ;;We only have optimized accessors for 3 dim tensor or less so we always want to
    ;;break the problem down into 3 dims or less.
    (if (> n-dims 3)
      (let [sub-tens (slice buffer (- n-dims 3))
            sub-buf-size (dtype-base/ecount (first sub-tens))]
        (loop [sidx sidx
               acc acc]
          (if (and (< sidx eidx) (not (reduced? acc)))
            (let [next-sidx (min eidx (round (+ sidx sub-buf-size) sub-buf-size))
                  buf-idx (quot sidx sub-buf-size)
                  sub-local-sidx (rem sidx sub-buf-size)
                  sub-local-eidx (+ sub-local-sidx (- next-sidx sidx))]
              (recur next-sidx
                     (nd-reduce (sub-tens buf-idx) rfn acc sub-local-sidx sub-local-eidx)))
            acc)))
      (case n-dims
        0 acc
        1 (NDReduce/ndReduce1D buffer rfn acc sidx eidx)
        2 (NDReduce/ndReduce2D buffer (long (buf-shape 1)) rfn acc sidx eidx)
        3 (NDReduce/ndReduce3D buffer (long (buf-shape 1)) (long (buf-shape 2))
                               rfn acc sidx eidx)))))

(comment
  (nd-reduce (compute-tensor
              [3 3 3] (fn [y x c] (println y x c) (+ y x c)))
             + 0 7 10)
  )


(defmacro ^:private make-tensor-reader
  [datatype advertised-datatype n-dims n-elems per-pixel-op
   output-shape shape-x shape-chan strides]
  (let [{:keys [read-fn nd-read-fn read-type cast-fn reduce-fn]}
        (case datatype
          :int64 {:read-fn 'readLong
                  :nd-read-fn '.ndReadLong
                  :read-type 'tech.v3.datatype.LongReader
                  :cast-fn 'long
                  :reduce-fn 'longReduction}
          :float64 {:read-fn 'readDouble
                    :nd-read-fn '.ndReadDouble
                    :read-type 'tech.v3.datatype.DoubleReader
                    :cast-fn 'double
                    :reduce-fn 'doubleReduction}
          {:read-fn 'readObject
           :nd-read-fn '.ndReadObject
           :read-type 'tech.v3.datatype.ObjectReader
           :cast-fn 'identity
           :reduce-fn 'reduce})
        invoker (case datatype
                  :int64 '.invokePrim
                  :float64 '.invokePrim
                  '.invoke)]
    `(reify ~read-type
       (elemwiseDatatype [rdr#] ~advertised-datatype)
       (lsize [rdr#] ~n-elems)
       (subBuffer [rdr# sidx# eidx#]
         (ChunkedList/sublistCheck sidx# eidx# ~n-elems)
         (let [nne# (- eidx# sidx#)]
           (reify ~read-type
             (elemwiseDatatype [rr#] ~advertised-datatype)
             (lsize [rr#] nne#)
             (~read-fn [rr# idx#] (~(symbol (str "." (name read-fn))) rdr# (+ idx# sidx#)))
             (subBuffer [rr# ssidx# seidx#]
               (ChunkedList/sublistCheck ssidx# seidx# nne#)
               (.subBuffer rdr# (+ sidx# ssidx#) (+ sidx# seidx#)))
             (reduce [this# rfn# init#]
               (nd-reduce ~per-pixel-op rfn# init# sidx# eidx#)))))
       (~read-fn [rdr# ~'idx]
         ~(case (long n-dims)
            1 `(~nd-read-fn ~per-pixel-op ~'idx)
            2 `(~nd-read-fn ~per-pixel-op
                (pmath// ~'idx ~shape-chan)
                (rem ~'idx ~shape-chan))
            3 `(let [c# (pmath/rem ~'idx ~shape-chan)
                     xy# (pmath// ~'idx ~shape-chan)
                     x# (pmath/rem xy# ~shape-x)
                     y# (pmath// xy# ~shape-x)]
                 (~nd-read-fn ~per-pixel-op y# x# c#))
            `(~cast-fn (.ndReadObjectIter ~per-pixel-op (shape-stride-reader
                                                         ~output-shape ~strides ~'idx)))))
       (reduce [this# rfn# init#]
         (nd-reduce ~per-pixel-op rfn# init# 0 ~n-elems)))))


(defn nd-buffer->buffer-reader
  ^Buffer [^NDBuffer b]
  (let [b-dtype (dtype-base/elemwise-datatype b)
        b-shape (.shape b)
        n-elems (long (apply * (.shape b)))
        shape-chan (long (last b-shape))
        shape-x (long (or (last (butlast b-shape))
                          0))
        strides (dims-analytics/shape-ary->strides b-shape)]
    (case (.rank b)
      1 (case (casting/simple-operation-space (dtype-base/elemwise-datatype b))
          :int64 (make-tensor-reader :int64 b-dtype 1 n-elems b b-shape shape-x shape-chan strides)
          :float64 (make-tensor-reader :float64 b-dtype 1 n-elems b b-shape shape-x shape-chan strides)
          (make-tensor-reader :object b-dtype 1 n-elems b b-shape shape-x shape-chan strides))
      2 (case (casting/simple-operation-space (dtype-base/elemwise-datatype b))
          :int64 (make-tensor-reader :int64 b-dtype 2 n-elems b b-shape shape-x shape-chan strides)
          :float64 (make-tensor-reader :float64 b-dtype 2 n-elems b b-shape shape-x shape-chan strides)
          (make-tensor-reader :object b-dtype 2 n-elems b b-shape shape-x shape-chan strides))
      3 (case (casting/simple-operation-space (dtype-base/elemwise-datatype b))
          :int64 (make-tensor-reader :int64 b-dtype 3 n-elems b b-shape shape-x shape-chan strides)
          :float64 (make-tensor-reader :float64 b-dtype 3 n-elems b b-shape shape-x shape-chan strides)
          (make-tensor-reader :object b-dtype 3 n-elems b b-shape shape-x shape-chan strides))
      (case (casting/simple-operation-space (dtype-base/elemwise-datatype b))
        :int64 (make-tensor-reader :int64 b-dtype 4 n-elems b b-shape shape-x shape-chan strides)
        :float64 (make-tensor-reader :float64 b-dtype 4 n-elems b b-shape shape-x shape-chan strides)
        (make-tensor-reader :object b-dtype 4 n-elems b b-shape shape-x shape-chan strides)))))


(defmacro typed-compute-tensor
  "Fastest possible inline compute tensor.  The code to generate the next
  element is output inline into the tensor definition.


  For the 4 argument version to work, shape must be compile
  time introspectable object with count so for instance `[a b c]` will work
  but item-shape will throw an exception.


  * `:datatype` - One of #{:int64 :float64} or :object is assumed.  This indicates
    the tensor interface definition and read operations that will be implemented.
    See 'java/tech/v3/datatype/[Long|Double|Object]TensorReader.java.
  * `:advertised-datatype` - Datatype you will tell the world.
  * `:rank` - compile time introspectable rank.  Indicates which ndReadX overloads
     will be implemented.
  * `:shape` - Shape of the output tensor.
  * `:op-code-args` - Op code arguments.  Expected to be a vector of argument
     names such as `[y x c].  Let destructuring is *NOT* supported beyond 3
     variables at this time.!!!`.
  * `:op-code` - Code which executes the read operation.

  Results in an implementation of NDBuffer which efficiently performs a 1,2 or 3 dimension
  ND read operation."
  ([datatype advertised-datatype rank shape op-code-args op-code]
   (let [{:keys [nd-read-fn read-type _cast-fn]}
         (case datatype
           :int64 {:nd-read-fn 'ndReadLong
                   :read-type 'tech.v3.datatype.LongTensorReader
                   :cast-fn 'long}
           :float64 {:read-fn 'readDouble
                     :nd-read-fn 'ndReadDouble
                     :read-type 'tech.v3.datatype.DoubleTensorReader
                     :cast-fn 'double}
           {:nd-read-fn 'ndReadObject
            :read-type 'tech.v3.datatype.ObjectTensorReader
            :cast-fn 'identity})

         rev-args (if (sequential? op-code-args)
                    (reverse op-code-args)
                    [])
         c (first rev-args)
         x (second rev-args)
         y (last rev-args)]
     `(let [shape# (vec ~shape)
            rank# (long ~rank)
            n-elems# (long (apply * shape#))
            dims# (dims/dimensions shape#)]
        (reify
          dtype-proto/PECount
          (ecount [this#] (.lsize this#))
          ~read-type
          (elemwiseDatatype [tr#] ~advertised-datatype)
          (shape [tr#] shape#)
          (dimensions [tr#] dims#)
          (indexSystem [tr#] (dims/->global->local dims#))
          (rank [tr#] rank#)
          (bufferIO [tr#] (nd-buffer->buffer-reader tr#))
          ;;Implement typed read access
          ~@(case (long rank)
              1 [`(~nd-read-fn [tr# ~c] ~op-code)
                 `(ndReadObjectIter
                   [tr# idx-seq#]
                   (if (== 1 (count idx-seq#))
                     (.ndReadObject tr# (first idx-seq#))
                     (errors/throwf "n-dims is 1, %d passed in" (count idx-seq#))))]
              2 [`(~nd-read-fn [tr# ~x ~c] ~op-code)
                 `(ndReadObject [tr# c#]
                                (dtype-proto/select tr# c#))
                 `(ndReadObjectIter
                   [tr# idx-seq#]
                   (case (count idx-seq#)
                     1 (.ndReadObject tr# (first idx-seq#))
                     2 (.ndReadObject tr# (first idx-seq#) (second idx-seq#))
                     (errors/throwf "n-dims is 2, %d passed in" (count idx-seq#))))]
              3 [`(~nd-read-fn [tr# ~y ~x ~c] ~op-code)
                 `(ndReadObject [tr# c#]
                                (dtype-proto/select tr# [c#]))
                 `(ndReadObject [tr# x# c#]
                                (dtype-proto/select tr# [x# c#]))
                 `(ndReadObjectIter
                   [tr# idx-seq#]
                   (case (count idx-seq#)
                     1 (.ndReadObject tr# (first idx-seq#))
                     2 (.ndReadObject tr# (first idx-seq#) (second idx-seq#))
                     3 (.ndReadObject tr# (first idx-seq#) (second idx-seq#) (last idx-seq#))
                     (errors/throwf "n-dims is 3, %d passed in" (count idx-seq#))))]
              [`(ndReadObjectIter [tr# indexes#]
                                  (if (== (count indexes#) ~rank)
                                    (let [~op-code-args indexes#]
                                      ~op-code)
                                    (dtype-proto/select tr# indexes#)))
               `(ndReadObject [tr# c#]
                              (dtype-proto/select tr# [c#]))
               `(ndReadObject [tr# x# c#]
                              (dtype-proto/select tr# [x# c#]))
               `(ndReadObject [tr# y# x# c#]
                              (dtype-proto/select tr# [y# x# c#]))])
          (iterator [tr#]
            (.iterator ^java.util.List (dtype-proto/slice tr# 1 false)))
          Object
          (toString [tr#] (tens-pp/tensor->string tr#))))))
  ([advertised-datatype rank shape op-code-args op-code]
   (case (casting/simple-operation-space advertised-datatype)
     :int64 `(typed-compute-tensor :int64 ~advertised-datatype ~rank ~shape ~op-code-args ~op-code)
     :float64 `(typed-compute-tensor :float64 ~advertised-datatype ~rank ~shape ~op-code-args ~op-code)
     `(typed-compute-tensor :object ~advertised-datatype ~rank ~shape ~op-code-args ~op-code)))
  ([advertised-datatype shape op-code-args op-code]
   (case (count shape)
     1 `(typed-compute-tensor ~advertised-datatype 1 ~shape ~op-code-args ~op-code)
     2 `(typed-compute-tensor ~advertised-datatype 2 ~shape ~op-code-args ~op-code)
     3 `(typed-compute-tensor ~advertised-datatype 3 ~shape ~op-code-args ~op-code)
     `(typed-compute-tensor ~advertised-datatype 4 ~shape ~op-code-args ~op-code))))


(defn compute-tensor
  "Create a new tensor which calls into op for every operation.
  Op will receive n-dimensional long arguments and the result will be
  `:unchecked-cast`ed to whatever datatype the tensor is reporting.

Example:

```clojure
user> (require '[tech.v3.tensor :as dtt])
nil
user> (dtt/compute-tensor [2 2] (fn [& args] (vec args)) :object)
#tech.v3.tensor<object>[2 2]
[[[0 0] [0 1]]
 [[1 0] [1 1]]]
user> (dtt/compute-tensor [2 2 2] (fn [& args] (vec args)) :object)
#tech.v3.tensor<object>[2 2 2]
[[[[0 0 0] [0 0 1]]
  [[0 1 0] [0 1 1]]]
 [[[1 0 0] [1 0 1]]
  [[1 1 0] [1 1 1]]]]
```"
  ([shape per-pixel-op datatype]
   (let [shape (vec shape)
         op-space (casting/simple-operation-space datatype)
         n-dims (count shape)]
     (case op-space
       :int64
       (case n-dims
         1 (typed-compute-tensor :int64 datatype 1 shape
                                 [c]
                                 (unchecked-long (per-pixel-op c)))
         2 (typed-compute-tensor :int64 datatype 2 shape
                                 [x c]
                                 (unchecked-long (per-pixel-op x c)))
         3 (typed-compute-tensor :int64 datatype 3 shape
                                 [y x c]
                                 (unchecked-long (per-pixel-op y x c)))
         (typed-compute-tensor :int64 datatype 4 shape
                               indexes
                               (unchecked-long (apply per-pixel-op indexes))))
       :float64
       (case n-dims
         1 (typed-compute-tensor :float64 datatype 1 shape
                                 [c]
                                 (unchecked-double (per-pixel-op c)))
         2 (typed-compute-tensor :float64 datatype 2 shape
                                 [x c]
                                 (unchecked-double (per-pixel-op x c)))
         3 (typed-compute-tensor :float64 datatype 3 shape
                                 [y x c]
                                 (unchecked-double (per-pixel-op y x c)))
         (typed-compute-tensor :float64 datatype 4 shape
                               indexes
                               (unchecked-double (apply per-pixel-op indexes))))
       ;;fallback to object
       (case n-dims
         1 (typed-compute-tensor :object datatype 1 shape
                                 [c]
                                 (per-pixel-op c))
         2 (typed-compute-tensor :object datatype 2 shape
                                 [x c]
                                 (per-pixel-op x c))
         3 (typed-compute-tensor :object datatype 3 shape
                                 [y x c]
                                 (per-pixel-op y x c))
         (typed-compute-tensor :object datatype 4 shape
                               indexes
                               (apply per-pixel-op indexes))))))
  ([output-shape per-pixel-op]
   (compute-tensor output-shape per-pixel-op
                   (dtype-base/elemwise-datatype per-pixel-op))))


(defn- as-nd-buffer ^NDBuffer [item] item)
(defmacro ^:private tens-copy-nd
  [datatype ndims src dst]
  `(let [~'src (as-nd-buffer ~src)
         ~'dst (as-nd-buffer ~dst)
         ~'src-shape (dtype-base/shape ~'src)
         ~'ny ~(if (= 3 (long ndims))
                 `(long (first ~'src-shape))
                 0)
         ~'nx ~(if (>= (long ndims) 2)
                 `(long (nth ~'src-shape ~(- (long ndims) 2)))
                 0)
         ~'nc (long (last ~'src-shape))]
     ~(case (long ndims)
        2 `(parallel-for/parallel-for
            ~'x
            ~'nx
            (dotimes [~'c ~'nc]
              ~(case datatype
                 :int64 `(.ndWriteLong ~'dst ~'x ~'c (.ndReadLong ~'src ~'x ~'c))
                 :float64 `(.ndWriteDouble ~'dst ~'x ~'c (.ndReadDouble ~'src ~'x ~'c))
                 :object `(.ndWriteObject ~'dst ~'x ~'c (.ndReadObject ~'src ~'x ~'c)))))
        3 `(parallel-for/parallel-for
            ~'y
            ~'ny
            (dotimes [~'x ~'nx]
              (dotimes [~'c ~'nc]
                ~(case datatype
                   :int64 `(.ndWriteLong ~'dst ~'y ~'x ~'c (.ndReadLong ~'src ~'y ~'x ~'c))
                   :float64 `(.ndWriteDouble ~'dst ~'y ~'x ~'c (.ndReadDouble ~'src ~'y ~'x ~'c))
                   :object `(.ndWriteObject ~'dst ~'y ~'x ~'c (.ndReadObject ~'src ~'y ~'x ~'c)))))))
     ~dst))


(defn nd-copy!
  "similar to tech.v3.datatype/copy! except this copy is ND aware and
  parallelizes over the outermost dimension.  This useful for compute tensors.
  If you have tensors such as images, see `tensor-copy!`."
  [src dst]
  (errors/when-not-error (and (instance? NDBuffer src)
                              (instance? NDBuffer dst))
    "Both arguments must be tensors.")
  (errors/when-not-errorf (= (dtype-base/shape src) (dtype-base/shape dst))
    "Source (%s) and destination (%s) shapes do not match."
    (dtype-base/shape src) (dtype-base/shape dst))
  (let [^NDBuffer src src
        ^NDBuffer dst dst
        src-rank (.rank src)
        op-space (casting/simple-operation-space
                  (dtype-base/elemwise-datatype src)
                  (dtype-base/elemwise-datatype dst))]
    (if (and (or (== src-rank 2)
                 (== src-rank 3))
             (#{:int64 :float64 :object} op-space))
      ;;Cases where ND-copy is actually defined.
      (case src-rank
        2 (case op-space
            :int64 (tens-copy-nd :int64 2 src dst)
            :float64 (tens-copy-nd :float64 2 src dst)
            :object (tens-copy-nd :object 2 src dst))
        3 (case op-space
            :int64 (tens-copy-nd :int64 3 src dst)
            :float64 (tens-copy-nd :float64 3 src dst)
            :object (tens-copy-nd :object 3 src dst)))
      (dtype-cmc/copy! src dst))))


(defn ensure-native
  "Ensure this tensor is native backed and packed.
  Item is cloned into a native tensor with the same datatype
  and :resource-type :auto by default.

  Options are the same as clone with the exception of :resource-type.

  * `:resource-type` - Defaults to :auto - used as `tech.v3.resource/track track-type`."
  ([tens options]
   (let [options
         (-> options
             (update :resource-type #(or % :auto))
             (update :datatype #(or % (dtype-base/elemwise-datatype tens)))
             (assoc :container-type :native-heap)
             ;;We are going to immediately overwrite the tensor..
             (assoc :uninitialized? true))]
     (if (and (dtype-base/as-native-buffer tens)
              (= (dtype-base/elemwise-datatype tens) (:datatype options)))
       (ensure-tensor tens)
       (apply clone tens (->> (seq options)
                              (apply concat))))))
  ([tens]
   (ensure-native tens nil)))


(defn native-tensor
  "Create a new native-backed tensor with a :resource-type :auto default
  resource type.

  Options are the same as new-tensor with some additions:

  * `:resource-type` - Defaults to :auto - used as `tech.v3.resource/track track-type`.
  * `:uninitialized?` - Defaults to false - do not 0-initialize the memory."
  ([shape datatype options]
   (let [options (-> options
                     (update :resource-type #(or % :auto))
                     (assoc :container-type :native-heap)
                     (assoc :datatype datatype))]
     (apply new-tensor shape (->> (seq options)
                                  (apply concat)))))
  ([shape datatype]
   (native-tensor shape datatype nil))
  ([shape]
   (native-tensor shape :float64 nil)))


(defn reduce-axis
  "Reduce a tensor along an axis using reduce-fn on the elemwise entries.


  * reduce-fn - lazily applied reduction applied to each input.  Inputs are
    1-dimensional vectors.  Use clone to force the operation.
  * tensor - input tensor to use.
  * axis - Defaults to -1 meaning the last axis.  So the default would
    reduce across the rows of a matrix.
  * res-dtype - result datatype, defaults to the datatype of the incoming
    tensor.

Example:

```clojure
user> t
#tech.v3.tensor<object>[4 3]
[[0  1  2]
 [3  4  5]
 [6  7  8]
 [9 10 11]]
user> (dtt/reduce-axis dfn/sum t 0)
#tech.v3.tensor<object>[3]
[18.00 22.00 26.00]
user> (dtt/reduce-axis dfn/sum t 1)
#tech.v3.tensor<object>[4]
[3.000 12.00 21.00 30.00]
user> (dtt/reduce-axis dfn/sum t)
#tech.v3.tensor<object>[4]
[3.000 12.00 21.00 30.00]
user> (dtt/reduce-axis dfn/sum t 0 :float64)
#tech.v3.tensor<float64>[3]
[18.00 22.00 26.00]


user> (def t (dtt/new-tensor [2 3 5]))
#'user/t
user> (dtype/shape (dtt/reduce-axis dfn/sum t 0))
[3 5]
user> (dtype/shape (dtt/reduce-axis dfn/sum t 1))
[2 5]
user> (dtype/shape (dtt/reduce-axis dfn/sum t 2))
[2 3]
```

  For the opposite - adding dimensions via repetition - see [[broadcast]]."
  ([reduce-fn tensor axis res-dtype]
   (let [rank (count (dtype-base/shape tensor))
         dec-rank (dec rank)
         res-dtype (or res-dtype (dtype-base/elemwise-datatype tensor))
         axis (long axis)
         axis (if (>= axis 0)
                axis
                (+ rank axis))
         ;;Get a relative set of indexes into the original shape that we
         ;;will use for transpose to move the reduction dimension to the
         ;;last or 'row' position.
         shape-idxes (remove #(= axis %) (range rank))
         orig-shape (dtype-base/shape tensor)
         ;;transpose the tensor so the reduction axis is the last one
         tensor (if-not (= dec-rank axis)
                  (transpose tensor (concat shape-idxes [axis]))
                  tensor)
         ;;slice to produce a sequence of rows
         slices (slice tensor dec-rank)
         ;;Result shape is the original shape minus the reduction axis.
         result-shape (mapv orig-shape shape-idxes)]
     (-> (emap/emap reduce-fn res-dtype slices)
         ;;reshape to the result shape
         (reshape result-shape))))
  ([reduce-fn tensor axis]
   (reduce-axis reduce-fn tensor axis nil))
  ([reduce-fn tensor]
   (reduce-axis reduce-fn tensor -1 nil)))


(comment
  (export-symbols/write-api! 'tech.v3.tensor-api
                             'tech.v3.tensor
                             "src/tech/v3/tensor.clj"
                             nil)
  )
