# dtype-next Cheatsheet

The old cheatsheet has been moved to [overview](https://cnuernber.github.io/dtype-next/overview.html).

Most of these functions are accessible via the `[tech.v3.datatype :as dtype]`
namespace.  When another namespace is required, it is specified separately.


## Containers


Containers are mutable storage of primitive datatypes.


* [make-container](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-make-container) - make a native heap or java heap based container.
* [make-list](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-make-list) - make a new efficient implementation of PrimitveList that stores data
  in a single contigous container.  Analog of java.util.ArrayList.
* [clone](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-clone) - Clone efficiently copies the data into a new container.
* [copy!](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-copy.21) - Copy data between containers.
* [coalesce!](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-coalesce.21) - Coalesce a sequence of containers of data into one pre-sized container.
* [tech.v3.datatype.mmap/mmap-file](https://cnuernber.github.io/dtype-next/tech.v3.datatype.mmap.html#var-mmap-file) - MMap a file, returning a native heap based
  container.
* [elemwise-datatype](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-elemwise-datatype) - Get the datatype of this buffer.
* [ecount](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-ecount) - the count of elements in the object.  This function returns a long (as
  opposed to int), is extensible to new objects and works on more objects than
  `clojure.core.count`.
* [shape](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-shape) - Return the ND shape of the object.



## Getting a Buffer/Reader

The buffer abstraction is the base efficient random access read/write abstraction
available in the library.  Most operations are implemented in terms of buffers.
Readers are buffers that do not support write - `(= false (.supportsWrite buf))`.
Readers and Buffers implement Indexed and IFn interfaces so they can be used as
functions of their indexes, be destructured, and used with `nth`.  Nearly anything
can be turned into a reader - persistent vectors, java arrays, anything deriving
from both java.util.List and java.util.RandomAccess or anything in-place convertible
to a java array or a native buffer.


* [as-buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-as-buffer), [->buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var--.3Ebuffer) - type hinted to return a [Buffer](https://github.com/cnuernber/dtype-next/blob/d04c309bd565292c1c3d9880b4bbb80b6ff9478e/java/tech/v3/datatype/Buffer.java).  `as-buffer` can return nil.
* [as-reader](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-as-reader), [->reader](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var--.3Ereader) - type hinted to return a Buffer.  `as-reader` can return nil.
* [elemwise-cast](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-elemwise-cast) - Perform a checked runtime cast operation upon read and advertise
  a new datatype to the system.
* [const-reader](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-const-reader) - Create a new buffer of a given length that always returns a const
  value.
* [make-reader](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-make-reader) - Create (reify) a new reader of a given datatype.  User provides inline
  code that converts from `idx` to a correctly typed value.  The ND analogue of this
  function is `compute-tensor`.


## Getting a Known Container Type

* [->array](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var--.3Earray), [->(*)-array](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var--.3Ebyte-array) Convert to a java array of a known datatype.
* [as-array-buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-as-array-buffer), [->array-buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var--.3Earray-buffer) - Convert to an 'array-buffer' a generalized
   (allows sub-buffer) concrete type used for all java arrays.
* [as-native-buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-as-native-buffer), [->native-buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var--.3Enative-buffer) - Convert to a native buffer.  This pathway
   forms the basis for numpy/julia ND integrations.


## Manipulating Buffers

We provide a few simple base methods to interact with buffers.  These will
automatically convert their input to a buffer so `->buffer` is not required.


* [indexed-buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-indexed-buffer) - Return a new buffer indexed via the integer indexes provided.
* [concat-buffers](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-concat-buffers) - in-place concatenate buffers.  Sometimes it will be faster to just
  create a new container.
* [sub-buffer](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-sub-buffer) - Take a contiguous range of indexes and return a new buffer that shares
  the underlying backing store.

## Lazy Elementwise Operations

Elemwentwise operations are lazily done upon read of the index and are not cached.
They are instant on large vectors but to 'realize' the operation into a new
container you will need to use 'clone' or 'copy!'.  This allows chaining together
multiple elementwise operations into one concrete parallelized operation at runtime
-- this is a cheap, simple form of 'combining kernels' that takes advantage of quick
vtable calls in order to be efficient.  When high-performance matters, there are
simple pathways to [inline operations](https://github.com/cnuernber/dtype-next/blob/d04c309bd565292c1c3d9880b4bbb80b6ff9478e/test/tech/v3/tensor/integration_test.clj#L49) and
from there to hand-written code that is as efficient as possible on the jvm.

* [emap](https://cnuernber.github.io/dtype-next/tech.v3.datatype.html#var-emap) - elemwise-map a function that performs the mapping upon each elemwise read
  returning a new reader.
* [tech.v3.datatype.functional](https://cnuernber.github.io/dtype-next/tech.v3.datatype.functional.html) - Namespace of elementwise operations along
  with a few reductions.  `dfn` also includes a few descriptive statistics operations.

## Index Space Operations

Working in index space is often the most efficient way to work as it involves
operations tailored to either `:int32` (int) or `:int64` (long) values and sets
of values.  These operations form a primary technical facility used by
tech.ml.dataset.  All of these operations are found in `tech.v3.datatype.argops`.
All of these operations return indexes that point to the query results and are meant
to be used in conjunction with `indexed-buffer` in order to re-index the underlying
data.

* [argmin](https://cnuernber.github.io/dtype-next/tech.v3.datatype.argops.html#var-argmin), [argmax](https://cnuernber.github.io/dtype-next/tech.v3.datatype.argops.html#var-argmax) - (serial) index of last min element, index of last max element.
* [binary-search](https://cnuernber.github.io/dtype-next/tech.v3.datatype.argops.html#var-binary-search) - (serial) return index of insert position for element.  May return
  n-elems in the case the element greater than any elements in the input data.
* [argfilter](https://cnuernber.github.io/dtype-next/tech.v3.datatype.argops.html#var-argfilter) - (parallel) - return a container of indexes that filter the data
  according to filter-fn.
* [argsort](https://cnuernber.github.io/dtype-next/tech.v3.datatype.argops.html#var-argsort) - (parallel) - return of container of indexes that sort the data.
* [arggroup](https://cnuernber.github.io/dtype-next/tech.v3.datatype.argops.html#var-arggroup), [arggroup-by](https://cnuernber.github.io/dtype-next/tech.v3.datatype.argops.html#var-arggroup-by) - (parallel) - Highly optimized method to return an implementation of
  java.util.Map where the keys are the result of group-fn and the values are a
  container of indexes of the respective source elements.


## High Performance Aggregations


These reductions are designed to allow a relatively simple api to high performance
group-by type operations that are useful to aggregate data.  These provide a lower
level interface to allow direct aggregations as opposed to the index-space
aggregations of `tech.v3.datatype.functional/arggroup-by`.  These are found in the
`tech.v3.datatype.reductions` namespace.

* [ordered-group-by-reduce](https://cnuernber.github.io/dtype-next/tech.v3.datatype.reductions.html#var-ordered-group-by-reduce), [unordered-group-by-reduce](https://cnuernber.github.io/dtype-next/tech.v3.datatype.reductions.html#var-unordered-group-by-reduce) - Reduce data into either a
  java.util.HashMap (ordered) or java.util.concurrent.ConcurrentHashMap (unordered)
  via transformation provided via the provided [IndexReduction](https://github.com/cnuernber/dtype-next/blob/d04c309bd565292c1c3d9880b4bbb80b6ff9478e/java/tech/v3/datatype/IndexReduction.java).  Note this interface provides both a per-index call called in each cpu thread and an aggregate-reduction call to merge the per-thead contexts.


## High Performance Parallelization Primitives


We attempt to provide a simple, axiomatic set of primitives to perform as efficient of
a reduction, filtering, or mapping operation as the JVM is capable of doing.  These
are found in the low-level namespace [tech.v3.parallel.for](https://cnuernber.github.io/dtype-next/tech.v3.parallel.for.html).


* [indexed-map-reduce](https://cnuernber.github.io/dtype-next/tech.v3.parallel.for.html#var-indexed-map-reduce) - Efficiently iterate over a range of integer indexes.
Indexes are traversed in order in a few of parallelization groups as possible and
optionally reduce with the provided reducer.  This simple design attempts to ensure
the minimal parallelization overhead along with giving the underlying hardware the
best possible chance to predict the next bit of data that will be accessed.  Callers
can create stack variables for summations and aggregations leading to a high chance of
auto-vectorization for primitive operations.
* [spliterator-map-reduce](https://cnuernber.github.io/dtype-next/tech.v3.parallel.for.html#var-spliterator-map-reduce) - A similer design to indexed map reduce to be used
in situations (such as iterating over HashMap buckets) where indexed-map-reduce
cannot be used.  Spliterators are slightly more general than indexes and tend to work
in value space as opposed to index space making accessing multiple data sources such
as columns of a dataset more expensive.  They are, however, much more general and allow
filter steps to be done with abstraction to the caller's reduction mechanism.


## ND/Tensor Operations

The [tech.v3.tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html) namespace forms the basis of ND support built on the primitives
above with the addition of an index operator, the `dimensions` namespace that allows
efficient index-space permutations of the above buffers.  `dtype-next` is ND aware
in that the elemwise operations exposed in `tech.v3.datatype.functional` work on
tensors although at this point broadcasting is manually required.  Tensor implement
the java NDBuffer interface so low level code can use typesafe operations to perform
efficient mutations and aggregations built on tensors.  Tensor also implement
Clojure's IObj interface allowing them to work with `meta`, `with-meta`, and
`vary-meta`.


`dtype-next` is row-major.  The `->buffer` function, when applied to tensors, gives
you a row-major linearly indexed representation of the tensor.  This means that
images are stored natively and linearly indexing the tensor will be most efficient
if the data is row-wise accessed -- all of which is designed to pair with
indexed-map-reduce.


Put another way, given a Y rows by X columns buffered image
the most efficient general way to access the data is iterating over X in the
inner loop.  The `->buffer` function linearizes access in just this way.

These functions are found in the `tech.v3.tensor` namespace.  Note that there
are extension namespaces to allow zero-copy access to [buffered images](https://cnuernber.github.io/dtype-next/tech.v3.libs.buffered-image.html) and
[neanderthal matrixes](https://cnuernber.github.io/dtype-next/tech.v3.libs.neanderthal.html).


### Creation

* [->tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var--.3Etensor) - Create a new tensor copying data.  Optionally specify datatype and
  container type.
* [new-tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-new-tensor) - Create a new concrete tensor of zeros of a given shape.
* [ensure-tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-ensure-tensor) - Attempt a zero-copy conversion falling back to ->tensor when
  zero-copy is not available.
* [native-tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-native-tensor) - Create a native-heap-based tensor.
* [ensure-native](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-ensure-native) - If input is not native-heap-based, create a new tensor else return
  input.
* [reshape](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-reshape) - Reshape any buffer-able object into a tensor of a given shape.
* [compute-tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-compute-tensor) - Create a new N-dimensional tensor via a function that takes N
  long integer index arguments and returns a value.  The tensor definition is lazy - the function is called upon read of the value.  For this reason to make
  a compute tensor concrete `tech.v3.datatype/clone` may be used.


### Manipulation

NDBuffers implement Indexed and IFn allowing them to be destructured on their
outermost dimension and allowing a default slicing operation to happen if the
number of integer arguments to their IFn interface is less than the number of
dimensions.


* [select](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-select) - select a subrect of data.  Dimension-indexes can be specified via the
  keyword `:all`, a clojure range, or a convertible-to-long-reader object.  This can be
  used to crap an image or to do reorderings such as imagespace bga->rgb conversions.
* [transpose](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-transpose) - Generic in-place transpose dimensions to implement operations of the
  type `i,j,k` -> `k,j,i` for all orderings of `i,j,k`.  This can be used, for instance, to convert
  between channels-first planar representation of an image (2d planes of r,g,b,a) to a
  standard rgba-interleaved representation of an image.
* [reshape](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-reshape) - Reshape a tensor via interpreting it as ->buffer and applying a new
  dimension object.  Can be used to in-place create a tensor out of a persisent
  vector.
* [broadcast](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-broadcast) - Create a larger read-only tensor via repeating one or more dimensions.
* [slice](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-slice), [slice-right](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-slice-right) - Create a reader formed by iterating the left/right N dimensions
  in order.  Given a 2D matrix, `(slice mat 1)` returns the rows while
  `(slice-right mat 1)` returns the columns.


### Zero-Copy Integrations

Native heap backed tensor allow zero-copy conversions between systems such as numpy
and julia.  Implementing zero-copy is fairly straight-forward as the shared ABI is:

```clojure
#{:ptr ;; long ptr
  :elemwise-datatype ;; Datatype of buffer
  :shape ;; integer shape
  :strides ;; integer byte-wise per-dimension strides}
```


* [nd-buffer-descriptor->tensor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-nd-buffer-descriptor-.3Etensor) - given an ND buffer descriptor, return a tensor.
* [ensure-nd-buffer-descriptor](https://cnuernber.github.io/dtype-next/tech.v3.tensor.html#var-ensure-nd-buffer-descriptor) - check if a given tensor
  supports zero-copy conversion to an nd buffer descriptor and perform that
  conversion.  Else copy tensor into a suitable buffer and create descriptor.


## Datetime Support

Datetime support is divided into useful type-hinted long constants, a set of scalar
functions to create and transform particular datetime types and a small set of
vectorized functions that can work on readers of datetime datatypes.

Scalar constructors are named after the type they construct and type hinted with their
return value.

All of the functions below are found in the [tech.v3.datatype.datetime](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html) namespace.  If the function is marked with `vectorized` then it can work on readers and scalars in a similar vein
as `+` in the functional namespace.


* `nanoseconds-in-*`, `milliseconds-in-*`, `seconds-in-*` - Type hinted constants to
make numeric conversions easier.
* `local-date`, `local-date-time`, `zoned-date-time`, `instant` - Scalar constructors
of  specific java.time types.
* [plus-temporal-amount](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html#var-plus-temporal-amount), [minus-temporal-amount](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html#var-minus-temporal-amount) - `vectorized` - add/subtract a temporal amount returning a new datetime object or reader.
* [between](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html#var-between) - `vectorized` - Find the amount of time between two datetime objects or readers of datetime
objects.
* [datetime->epoch](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html#var-datetime-.3Eepoch), [epoch->datetime](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html#var-epoch-.3Edatetime) - `vectorized` - convert to an epoch datatype such as milliseconds-since-epoch.
* [long-temporal-field](https://cnuernber.github.io/dtype-next/tech.v3.datatype.datetime.html#var-long-temporal-field) - `vectorized` - Return a specific temporal field such as :days-since-epoch from a datetime type or a reader of datetime types.
