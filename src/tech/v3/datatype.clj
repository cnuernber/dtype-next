(ns tech.v3.datatype
  ;;Autogenerated from tech.v3.datatype-api-- DO NOT EDIT
  "Base namespace for container creation and elementwise access of data"
  (:require [tech.v3.datatype-api]
            [tech.v3.datatype.argtypes]
            [tech.v3.datatype.array-buffer]
            [tech.v3.datatype.base]
            [tech.v3.datatype.casting]
            [tech.v3.datatype.const-reader]
            [tech.v3.datatype.copy-make-container]
            [tech.v3.datatype.dispatch]
            [tech.v3.datatype.emap]
            [tech.v3.datatype.io-concat-buffer]
            [tech.v3.datatype.io-indexed-buffer]
            [tech.v3.datatype.native-buffer])
  (:refer-clojure :exclude [cast reverse]))

(defn ->array
  "Perform a NaN-aware conversion into an array.  Default
  nan-strategy is :keep.
  Nan strategies can be: [:keep :remove :exception]"
  ([datatype options item]
  (tech.v3.datatype.copy-make-container/->array datatype options item))
  ([datatype item]
  (tech.v3.datatype.copy-make-container/->array datatype item))
  ([item]
  (tech.v3.datatype.copy-make-container/->array item)))


(defn ->array-buffer
  "Perform a NaN-aware conversion into an array buffer.  Default
  nan-strategy is :keep.


  Options:

  * `:nan-strategy` - Nan strategies can be: [:keep :remove :exception].  `:remove` can
  result in a shorter array buffer than the input data.  Default is `:keep`."
  (^{:tag tech.v3.datatype.array_buffer.ArrayBuffer} [datatype options item]
  (tech.v3.datatype.copy-make-container/->array-buffer datatype options item))
  (^{:tag tech.v3.datatype.array_buffer.ArrayBuffer} [datatype item]
  (tech.v3.datatype.copy-make-container/->array-buffer datatype item))
  (^{:tag tech.v3.datatype.array_buffer.ArrayBuffer} [item]
  (tech.v3.datatype.copy-make-container/->array-buffer item)))


(defn ->buffer
  "Convert this item to a buffer implementation or throw an exception."
  (^{:tag tech.v3.datatype.Buffer} [item]
  (tech.v3.datatype.base/->buffer item)))


(defn ->byte-array
  "Efficiently convert nearly anything into a byte array."
  (^{:tag bytes} [data]
  (tech.v3.datatype.copy-make-container/->byte-array data)))


(defn ->char-array
  "Efficiently convert nearly anything into a char array."
  (^{:tag chars} [data]
  (tech.v3.datatype.copy-make-container/->char-array data)))


(defn ->double-array
  "Nan-aware conversion to double array.
  See documentation for ->array-buffer.  Returns a double array.
  options -
   * nan-strategy - :keep (default) :remove :exception"
  (^{:tag doubles} [options data]
  (tech.v3.datatype.copy-make-container/->double-array options data))
  (^{:tag doubles} [data]
  (tech.v3.datatype.copy-make-container/->double-array data)))


(defn ->float-array
  "Nan-aware conversion to double array.
  See documentation for ->array-buffer.  Returns a double array.
  options -
   * nan-strategy - :keep (default) :remove :exception"
  (^{:tag floats} [options data]
  (tech.v3.datatype.copy-make-container/->float-array options data))
  (^{:tag floats} [data]
  (tech.v3.datatype.copy-make-container/->float-array data)))


(defn ->int-array
  "Efficiently convert nearly anything into a int array."
  (^{:tag ints} [data]
  (tech.v3.datatype.copy-make-container/->int-array data)))


(defn ->iterable
  "Ensure this object is iterable.  If item is iterable, return the item.
  If the item is convertible to a buffer object , convert to it and
  return the object.  Else if the item is a scalar constant, return
  an infinite sequence of items."
  (^{:tag java.lang.Iterable} [item]
  (tech.v3.datatype.base/->iterable item)))


(defn ->long-array
  "Efficiently convert nearly anything into a long array."
  (^{:tag longs} [data]
  (tech.v3.datatype.copy-make-container/->long-array data)))


(defn ->native-buffer
  (^{:tag tech.v3.datatype.native_buffer.NativeBuffer} [data]
  (tech.v3.datatype-api/->native-buffer data)))


(defn ->reader
  "If this object has a read-only or read-write conversion to a buffer
  object return the buffer object.  Else throw an exception."
  (^{:tag tech.v3.datatype.Buffer} [item]
  (tech.v3.datatype.base/->reader item))
  (^{:tag tech.v3.datatype.Buffer} [item read-datatype]
  (tech.v3.datatype.base/->reader item read-datatype)))


(defn ->short-array
  "Efficiently convert nearly anything into a short array."
  (^{:tag shorts} [data]
  (tech.v3.datatype.copy-make-container/->short-array data)))


(defn ->vector
  "Copy a thing into a persistent vector."
  ([item]
  (tech.v3.datatype-api/->vector item)))


(defn ->writer
  "If this item is convertible to a PrimtiveIO writing the perform the
  conversion and return the interface.
  Else throw an excepion."
  (^{:tag tech.v3.datatype.Buffer} [item]
  (tech.v3.datatype.base/->writer item)))


(defn alloc-uninitialized
  "Allocate an uninitialized buffer.  See options for [[malloc]]."
  ([dtype ec]
  (tech.v3.datatype.native-buffer/alloc-uninitialized dtype ec))
  ([dtype ec options]
  (tech.v3.datatype.native-buffer/alloc-uninitialized dtype ec options)))


(defn alloc-zeros
  "Allocate a buffer of zeros.  See options for [[malloc]]."
  ([dtype ec]
  (tech.v3.datatype.native-buffer/alloc-zeros dtype ec))
  ([dtype ec options]
  (tech.v3.datatype.native-buffer/alloc-zeros dtype ec options)))


(defn arg-type
  "Return the type of a thing.  Types could be:
  :scalar
  :iterable
  :reader
  :tensor (reader with more than 1 dimension)"
  ([arg]
  (tech.v3.datatype.argtypes/arg-type arg)))


(defn as-array-buffer
  "If this item is convertible to a tech.v3.datatype.array_buffer.ArrayBuffer
  then convert it and return the typed buffer"
  (^{:tag tech.v3.datatype.array_buffer.ArrayBuffer} [item]
  (tech.v3.datatype.array-buffer/as-array-buffer item)))


(defn as-array-buffer-data
  (^{:tag tech.v3.datatype.ArrayBufferData} [item]
  (tech.v3.datatype-api/as-array-buffer-data item)))


(defn as-buffer
  "If this item is or has a conversion to an implementation of the Buffer
  interface then return the buffer interface for this object.
  Else return nil."
  (^{:tag tech.v3.datatype.Buffer} [item]
  (tech.v3.datatype.base/as-buffer item)))


(defn as-concrete-buffer
  "If this item is representable as a base buffer type, either native or array,
  return that buffer."
  ([item]
  (tech.v3.datatype.base/as-concrete-buffer item)))


(defn as-native-buffer
  "If this item is convertible to a tech.v3.datatype.native_buffer.NativeBuffer
  return it.  Return nil otherwise."
  (^{:tag tech.v3.datatype.native_buffer.NativeBuffer} [item]
  (tech.v3.datatype.native-buffer/as-native-buffer item)))


(defn as-native-buffer-data
  (^{:tag tech.v3.datatype.NativeBufferData} [item]
  (tech.v3.datatype-api/as-native-buffer-data item)))


(defn as-nd-buffer-descriptor
  "If this item is convertible to a buffer descriptor, convert it.  Else
  return nil.

  Buffer descriptors are a ND description of data.  For example, a native
  3x3 tensor has a buffer description like thus:

  ```clojure
  {:ptr 140330005614816
   :datatype :float64
   :endianness :little-endian
   :shape [3 3]
   :strides [24 8]}
  ```

  This design allows zero-copy transfer between neanderthal, numpy, tvm, etc."
  ([src-item]
  (tech.v3.datatype-api/as-nd-buffer-descriptor src-item)))


(defn as-reader
  "If this object has a read-only or read-write conversion to a primitive
  io object return the buffer object."
  (^{:tag tech.v3.datatype.Buffer} [item]
  (tech.v3.datatype.base/as-reader item))
  (^{:tag tech.v3.datatype.Buffer} [item read-datatype]
  (tech.v3.datatype.base/as-reader item read-datatype)))


(defn as-roaring-bitmap
  (^{:tag org.roaringbitmap.RoaringBitmap} [item]
  (tech.v3.datatype-api/as-roaring-bitmap item)))


(defn as-writer
  "If this item is convertible to a PrimtiveIO writing the perform the
  conversion and return the interface."
  (^{:tag tech.v3.datatype.Buffer} [item]
  (tech.v3.datatype.base/as-writer item)))


(defn cast
  "Perform a checked cast of a value to specific datatype."
  ([value datatype]
  (tech.v3.datatype.casting/cast value datatype)))


(defn clone
  "Clone an object.  Can clone anything convertible to a reader."
  ([item]
  (tech.v3.datatype-api/clone item)))


(defn clone-native
  "Extremely fast clone assuming src is a native buffer. Exception otherwise.
  See options for [[malloc]]."
  ([data options]
  (tech.v3.datatype.native-buffer/clone-native data options))
  ([data]
  (tech.v3.datatype.native-buffer/clone-native data)))


(defn coalesce!
  "Coalesce data from a raw sequence of things into a contiguous buffer.
  Returns the buffer; uses the copy-raw->item! pathway."
  ([buffer raw-data]
  (tech.v3.datatype-api/coalesce! buffer raw-data)))


(defn coalesce-blocks!
  "Copy a sequence of blocks of countable things into a larger
  countable thing."
  ([dst src-seq]
  (tech.v3.datatype-api/coalesce-blocks! dst src-seq)))


(defn concat-buffers
  "Concatenate a list of buffers into a single unified buffer.  This shares the data and it works when
  the number of buffers is relatively small.  The failure case for this is a large number of small buffers
  where it will be faster to simply copy all the data into a new buffer."
  ([datatype buffers]
  (tech.v3.datatype.io-concat-buffer/concat-buffers datatype buffers))
  ([buffers]
  (tech.v3.datatype.io-concat-buffer/concat-buffers buffers)))


(defn const-reader
  "Create a new reader that only returns the item for the provided indexes."
  (^{:tag tech.v3.datatype.Buffer} [item n-elems]
  (tech.v3.datatype.const-reader/const-reader item n-elems)))


(defn copy!
  "Mutably copy values from a src container into a destination container.
  Returns the destination container."
  ([src dst options]
  (tech.v3.datatype.copy-make-container/copy! src dst options))
  ([src dst]
  (tech.v3.datatype.copy-make-container/copy! src dst)))


(defn copy-raw->item!
  "Copy raw data into a buffer.  Data may be a sequence of numbers or a sequence
  of containers.  Data will be coalesced into the buffer.  Returns a tuple of:
  [buffer final-offset]."
  ([raw-data buffer options]
  (tech.v3.datatype-api/copy-raw->item! raw-data buffer options))
  ([raw-data buffer]
  (tech.v3.datatype-api/copy-raw->item! raw-data buffer)))


(defn datatype
  "Return this object's actual datatype.
  **This is not the same as the DEPRECATED get-datatype.  That function maps
  to `elemwise-datatype`.**
  This maps to the parameterization of the object, so for instance a list of ints
  might be:
```clojure
  {:container-type :list :elemwise-datatype :int32}
```
  Defaults to this object's elemwise-datatype."
  ([item]
  (tech.v3.datatype.base/datatype item)))


(defn ecount
  "Return the long number of elements in the object."
  (^{:tag long} [item]
  (tech.v3.datatype.base/ecount item)))


(defn elemwise-cast
  "Create a new thing by doing a checked cast from the old values
  to the new values upon at read time.  Return value will report
  new-dtype as it's elemwise-datatype."
  ([item new-dtype]
  (tech.v3.datatype.base/elemwise-cast item new-dtype)))


(defn elemwise-datatype
  "Return the datatype one would expect when iterating through a container
  of this type.  For scalars, return your elemental datatype."
  ([item]
  (tech.v3.datatype.base/elemwise-datatype item)))


(defn emap
  "Elemwise map:

  1. If input are all scalars, results in a scalar.
  2. If any inputs are iterables, results in an iterable.
  3. Either a reader or a tensor is returned.  All input shapes
     have to match.

  res-dtype is nil it is deduced from unifying the argument datatypes meaning
  map-fn, unless it is typed, is assumed to keep data in the same numeric space
  as the input."
  ([map-fn res-dtype x]
  (tech.v3.datatype.emap/emap map-fn res-dtype x))
  ([map-fn res-dtype x y]
  (tech.v3.datatype.emap/emap map-fn res-dtype x y))
  ([map-fn res-dtype x y & args]
  (apply tech.v3.datatype.emap/emap map-fn res-dtype x y args)))


(defn ensure-native
  "If input is already a native buffer and has the same datatype as output,
  return input.  Else copy input into output and return output."
  ([input outbuf]
  (tech.v3.datatype.native-buffer/ensure-native input outbuf))
  ([input]
  (tech.v3.datatype.native-buffer/ensure-native input)))


(defn ensure-reader
  "Ensure item is randomly addressable.  This may copy the data into a randomly
  accessible container."
  (^{:tag tech.v3.datatype.Buffer} [item n-const-elems]
  (tech.v3.datatype.copy-make-container/ensure-reader item n-const-elems))
  (^{:tag tech.v3.datatype.Buffer} [item]
  (tech.v3.datatype.copy-make-container/ensure-reader item)))


(defn ensure-serializeable
  "Ensure this is an efficiently serializeable datatype object.
  For nippy support across buffers and tensors require `tech.v3.datatype.nippy`."
  ([item]
  (tech.v3.datatype-api/ensure-serializeable item)))


(defmacro export-symbols
  ([lib & args]
  `(tech.v3.datatype-api/export-symbols ~lib ~@args)))


(defn get-datatype
  "Legacy method, returns elemwise-datatype"
  ([item]
  (tech.v3.datatype-api/get-datatype item)))


(defn get-value
  "Get a value from an object via conversion to a reader."
  ([item idx]
  (tech.v3.datatype.base/get-value item idx)))


(defn indexed-buffer
  "Create a new Buffer implementation that indexes into a previous
  Buffer implementation via the provided indexes."
  (^{:tag tech.v3.datatype.Buffer} [indexes item]
  (tech.v3.datatype.io-indexed-buffer/indexed-buffer indexes item)))


(defn iterable?
  "Return true if this is either a instance of Iterable or an instance of
  java.util.streams.Stream."
  ([item]
  (tech.v3.datatype.base/iterable? item)))


(defn make-container
  "Make a container of a given datatype.  Options are container specific
  and in general unused.  Values will be copied into given container using
  the most efficient pathway possible."
  ([container-type datatype options elem-seq-or-count]
  (tech.v3.datatype.copy-make-container/make-container container-type datatype options elem-seq-or-count))
  ([container-type datatype elem-seq-or-count]
  (tech.v3.datatype.copy-make-container/make-container container-type datatype elem-seq-or-count))
  ([datatype elem-seq-or-count]
  (tech.v3.datatype.copy-make-container/make-container datatype elem-seq-or-count))
  ([elem-seq-or-count]
  (tech.v3.datatype.copy-make-container/make-container elem-seq-or-count)))


(defn make-list
  "Make an instance of a tech.v3.datatype.Buffer.  These have typed add*
  methods, implement tech.v3.datatype.Buffer, and a guaranteed in-place transformation
  to a concrete buffer (defaults to an array buffer).

  If an integer is passed in an empty list that has preallocated storage is returned.
  If a sequence of data is passed in then a non-empty list that contains just those elements
  is returned.

  Example:

```clojure
user> (require '[tech.v3.datatype :as dt])
nil
user> (dt/make-list :float32 10)
#list<float32>[0]
[]
user> (dt/make-list :float32 (range 10))
#list<float32>[10]
[0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000]
```"
  (^{:tag ham_fisted.IMutList} [datatype n-elems-or-data]
  (tech.v3.datatype-api/make-list datatype n-elems-or-data))
  (^{:tag ham_fisted.IMutList} [datatype]
  (tech.v3.datatype-api/make-list datatype)))


(defmacro make-reader
  "Make a reader.  Datatype must be a compile time visible object.
  read-op has 'idx' in scope which is the index to read from.  Returns a
  reader of the appropriate type for the passed in datatype.  Results are unchecked
  casted to the appropriate datatype.  It is up to *you* to ensure this is the result
  you want or throw an exception.

  This function creates a compile-time object that can allow maximum performance.  For
  most use cases this is probably overkill and 'emap' is a more ideal pathway that
  matches Clojure constructs a bit closer.

  reader-datatype must be a compile time constant but advertised datatype need not be.

user> (dtype/make-reader :float32 5 idx)
[0.0 1.0 2.0 3.0 4.0]
user> (dtype/make-reader :boolean 5 idx)
[true true true true true]
user> (dtype/make-reader :boolean 5 (== idx 0))
[true false false false false]
user> (dtype/make-reader :float32 5 (* idx 2))
 [0.0 2.0 4.0 6.0 8.0]"
  ([datatype n-elems read-op]
  `(tech.v3.datatype-api/make-reader ~datatype ~n-elems ~read-op))
  ([reader-datatype advertised-datatype n-elems read-op]
  `(tech.v3.datatype-api/make-reader ~reader-datatype ~advertised-datatype ~n-elems ~read-op)))


(defn make-reader-fn
  ([_datatype advertised-datatype n-elems read-fn]
  (tech.v3.datatype-api/make-reader-fn _datatype advertised-datatype n-elems read-fn)))


(defn malloc
  "Malloc memory.  If a desired buffer type is needed follow up with set-native-datatype.
  
  * n-bytes - the number of bytes to malloc
  * opts - Either a keyword datatype or an options map.

  Options:

  * `:resource-type` - defaults to `:gc` - maps to `:track-type` in `tech.v3.resource/track`
     but can also be set to nil in which case the data is not tracked this library will
     not clean it up.
  * `:datatype` - Datatype - defaults to :int8.  Datatype byte width must evenly divide n-bytes.
  * `:uninitialized?` - do not initialize to zero.  Use for perf in very very rare cases.
  * `:endianness` - Either `:little-endian` or `:big-endian` - defaults to platform.
  * `:log-level` - one of `#{:debug :trace :info :warn :error :fatal}` or nil if no logging
     is desired.  When enabled allocations and frees will be logged in the same manner as
     `tech.jna`."
  (^{:tag tech.v3.datatype.native_buffer.NativeBuffer} [n-bytes dtype-or-opts]
  (tech.v3.datatype.native-buffer/malloc n-bytes dtype-or-opts))
  (^{:tag tech.v3.datatype.native_buffer.NativeBuffer} [n-bytes]
  (tech.v3.datatype.native-buffer/malloc n-bytes)))


(defn prealloc-list
  "Make an list with preallocated storage.  This function exists to cause
  a compilation error if older versions of dtype-next are included and is
  the equivalent (in latest versions of dtype-next) to
  `(make-list datatype n-elems)`."
  (^{:tag tech.v3.datatype.Buffer} [datatype n-elems]
  (tech.v3.datatype-api/prealloc-list datatype n-elems)))


(defn reader-like?
  "Returns true if this argument type is convertible to a reader."
  ([argtype]
  (tech.v3.datatype.argtypes/reader-like? argtype)))


(defn reader?
  "True if this item is convertible to a read-only or read-write Buffer
  interface."
  ([item]
  (tech.v3.datatype.base/reader? item)))


(defn reverse
  "Reverse an sequence, range or reader.
  * If range, returns a new range.
  * If sequence, uses clojure.core/reverse
  * If reader, returns a new reader that performs an in-place reverse."
  ([item]
  (tech.v3.datatype-api/reverse item)))


(defn set-constant!
  "Set a contiguous region of a buffer to a constant value"
  ([item offset length value]
  (tech.v3.datatype.base/set-constant! item offset length value))
  ([item offset value]
  (tech.v3.datatype.base/set-constant! item offset value))
  ([item value]
  (tech.v3.datatype.base/set-constant! item value)))


(defn set-datatype
  "Legacy method.  Performs elemwise-cast."
  ([item new-dtype]
  (tech.v3.datatype-api/set-datatype item new-dtype)))


(defn set-native-datatype
  "Set the datatype of a native buffer.  n-elems will be recalculated."
  (^{:tag tech.v3.datatype.native_buffer.NativeBuffer} [item datatype]
  (tech.v3.datatype.native-buffer/set-native-datatype item datatype)))


(defn set-value!
  "Set a value on an object via conversion to a writer.  set-value can also take a tuple
  in which case a select operation will be done on the tensor and value applied to the
  result.  See also [[set-constant!]].

  Example:

```clojure
tech.v3.tensor.integration-test> (def test-tens (dtt/->tensor (->> (range 27)
                                     (partition 3)
                                     (partition 3))
                                {:datatype :float64}))

#'tech.v3.tensor.integration-test/test-tens
tech.v3.tensor.integration-test> test-tens
#tech.v3.tensor<float64>[3 3 3]
[[[0.000 1.000 2.000]
  [3.000 4.000 5.000]
  [6.000 7.000 8.000]]
 [[9.000 10.00 11.00]
  [12.00 13.00 14.00]
  [15.00 16.00 17.00]]
 [[18.00 19.00 20.00]
  [21.00 22.00 23.00]
  [24.00 25.00 26.00]]]
tech.v3.tensor.integration-test> (dtype/set-value! (dtype/clone test-tens) [:all :all (range 2)] 0)
#tech.v3.tensor<float64>[3 3 3]
[[[0.000 0.000 2.000]
  [0.000 0.000 5.000]
  [0.000 0.000 8.000]]
 [[0.000 0.000 11.00]
  [0.000 0.000 14.00]
  [0.000 0.000 17.00]]
 [[0.000 0.000 20.00]
  [0.000 0.000 23.00]
  [0.000 0.000 26.00]]]
tech.v3.tensor.integration-test> (def sv-tens (dtt/reshape (double-array [1 2 3]) [3 1]))
#'tech.v3.tensor.integration-test/sv-tens
tech.v3.tensor.integration-test> (dtype/set-value! (dtype/clone test-tens) [:all :all (range 2)] sv-tens)
#tech.v3.tensor<float64>[3 3 3]
[[[1.000 1.000 2.000]
  [2.000 2.000 5.000]
  [3.000 3.000 8.000]]
 [[1.000 1.000 11.00]
  [2.000 2.000 14.00]
  [3.000 3.000 17.00]]
 [[1.000 1.000 20.00]
  [2.000 2.000 23.00]
  [3.000 3.000 26.00]]]
```"
  ([item idx value]
  (tech.v3.datatype.base/set-value! item idx value)))


(defn shape
  "Return a persistent vector of the shape of the object.  Dimensions
  are returned from least-rapidly-changing to most-rapidly-changing."
  ([item]
  (tech.v3.datatype.base/shape item)))


(defn sub-buffer
  "Create a sub-buffer (view) of the given buffer."
  ([item offset length]
  (tech.v3.datatype.base/sub-buffer item offset length))
  ([item offset]
  (tech.v3.datatype.base/sub-buffer item offset)))


(defn unchecked-cast
  "Perform an unchecked cast of a value to specific datatype."
  ([value datatype]
  (tech.v3.datatype.casting/unchecked-cast value datatype)))


(defn vectorized-dispatch-1
  "Perform a vectorized dispatch meaning return a different object depending on the
  argument type of arg1.  If arg1 is scalar, use scalar-fn.  If arg1 is an iterable,
  use iterable-fn.  Finally if arg1 is a reader, use reader-fn.  The space the
  operation is to be performed in can be loosely set with :operation-space in the
  options.  The actual space the operation will be performed in (and the return type
  of the system) is a combination of :operation-space if it exists and the elemwise
  datatype of arg1.  See tech.v3.datatype.casting/widest-datatype for more
  information."
  ([scalar-fn iterable-fn reader-fn options arg1]
  (tech.v3.datatype.dispatch/vectorized-dispatch-1 scalar-fn iterable-fn reader-fn options arg1))
  ([scalar-fn iterable-fn reader-fn arg1]
  (tech.v3.datatype.dispatch/vectorized-dispatch-1 scalar-fn iterable-fn reader-fn arg1))
  ([scalar-fn reader-fn arg1]
  (tech.v3.datatype.dispatch/vectorized-dispatch-1 scalar-fn reader-fn arg1)))


(defn vectorized-dispatch-2
  "Perform a vectorized dispatch meaning return a different object depending on the
  argument types of arg1 and arg2.  If arg1 and arg2 are both scalar, use scalar-fn.
  If neither argument is a reader, use iterable-fn.  Finally if both arguments are
  readers, use reader-fn.  The space the operation is to be performed in can be
  loosely set with :operation-space in the options.  The actual space the operation
  will be performed in (and the return type of the system) is a combination of
  :operation-space if it exists and the elemwise datatypes of arg1 and arg2.  See
  tech.v3.datatype.casting/widest-datatype for more information."
  ([scalar-fn iterable-fn reader-fn options arg1 arg2]
  (tech.v3.datatype.dispatch/vectorized-dispatch-2 scalar-fn iterable-fn reader-fn options arg1 arg2))
  ([scalar-fn reader-fn arg1 arg2]
  (tech.v3.datatype.dispatch/vectorized-dispatch-2 scalar-fn reader-fn arg1 arg2)))


(defn writer?
  "True if this item is convertible to a Buffer interface that supports
  writing."
  ([item]
  (tech.v3.datatype.base/writer? item)))


