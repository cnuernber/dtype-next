(ns tech.v3.tensor
  ;;Autogenerated from tech.v3.tensor-api-- DO NOT EDIT
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
  (:require [tech.v3.tensor-api]
            [tech.v3.datatype.base]))

(defn ->DirectTensor
  "Positional factory function for class tech.v3.tensor_api.DirectTensor."
  ([buffer dimensions rank index-system cached-io y x c metadata]
  (tech.v3.tensor-api/->DirectTensor buffer dimensions rank index-system cached-io y x c metadata)))


(defn ->Tensor
  "Positional factory function for class tech.v3.tensor_api.Tensor."
  ([buffer dimensions rank index-system cached-io metadata]
  (tech.v3.tensor-api/->Tensor buffer dimensions rank index-system cached-io metadata)))


(defn ->jvm
  "Conversion to storage that is efficient for the jvm.
  Base storage is either jvm-array or persistent-vector."
  ([item & args]
  (apply tech.v3.tensor-api/->jvm item args)))


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
  (^{:tag tech.v3.tensor_api.Tensor} [data & args]
  (apply tech.v3.tensor-api/->tensor data args)))


(defn as-tensor
  "Attempts an in-place conversion of this object to a tech.v3.datatype.NDBuffer interface.
  For a guaranteed conversion, use ensure-tensor."
  (^{:tag tech.v3.datatype.NDBuffer} [data]
  (tech.v3.tensor-api/as-tensor data)))


(defn broadcast
  "Broadcase an element into a new (larger) shape.  The new shape's dimension
  must be even multiples of the old shape's dimensions.  Elements are repeated.

  See [[reduce-axis]] for the opposite operation."
  (^{:tag tech.v3.datatype.NDBuffer} [t new-shape]
  (tech.v3.datatype.base/broadcast t new-shape)))


(defn clone
  "Clone a tensor via copying the tensor into a new container.  Datatype defaults
  to the datatype of the tensor and container-type defaults to `:java-heap`.

  Options:

  * `:datatype` - Specify a new datatype to copy data into.
  * `:container-type` - Specify the container type of the new tensor.
     Defaults to `:jvm-heap`.
    * `:resource-type` - One of `tech.v3.resource/track` `:track-type` options.  If allocating
     native tensors, `nil` corresponds to `gc:`."
  (^{:tag tech.v3.datatype.NDBuffer} [tens & args]
  (apply tech.v3.tensor-api/clone tens args)))


(defn columns
  "Return the columns of the tensor in a randomly-addressable structure."
  (^{:tag java.util.List} [src]
  (tech.v3.tensor-api/columns src)))


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
  (tech.v3.tensor-api/compute-tensor shape per-pixel-op datatype))
  ([output-shape per-pixel-op]
  (tech.v3.tensor-api/compute-tensor output-shape per-pixel-op)))


(defn const-tensor
  "Construct a tensor from a value and a shape.  Data is represented efficiently via a const-reader."
  (^{:tag tech.v3.datatype.NDBuffer} [value shape]
  (tech.v3.tensor-api/const-tensor value shape)))


(defn construct-tensor
  "Construct an implementation of tech.v3.datatype.NDBuffer from a buffer and
  a dimensions object.  See dimensions/dimensions."
  (^{:tag tech.v3.tensor_api.Tensor} [buffer dimensions & args]
  (apply tech.v3.tensor-api/construct-tensor buffer dimensions args)))


(defn dimensions-dense?
  "Returns true of the dimensions of a tensor are dense, meaning no gaps due to
  striding."
  ([tens]
  (tech.v3.tensor-api/dimensions-dense? tens)))


(defn dims-suitable-for-desc?
  "Are the dimensions of this object suitable for use in a buffer description?
  breaks due to striding."
  ([item]
  (tech.v3.tensor-api/dims-suitable-for-desc? item)))


(defn ensure-native
  "Ensure this tensor is native backed and packed.
  Item is cloned into a native tensor with the same datatype
  and :resource-type :auto by default.

  Options are the same as clone with the exception of :resource-type.

  * `:resource-type` - Defaults to :auto - used as `tech.v3.resource/track track-type`."
  ([tens options]
  (tech.v3.tensor-api/ensure-native tens options))
  ([tens]
  (tech.v3.tensor-api/ensure-native tens)))


(defn ensure-nd-buffer-descriptor
  "Get a buffer descriptor from the tensor.  This may copy the data.  If you want to
  ensure sharing, use the protocol ->nd-buffer-descriptor function."
  ([tens]
  (tech.v3.tensor-api/ensure-nd-buffer-descriptor tens)))


(defn ensure-tensor
  "Create an implementation of tech.v3.datatype.NDBuffer from an
  object.  If possible, represent the data in-place."
  (^{:tag tech.v3.datatype.NDBuffer} [item]
  (tech.v3.tensor-api/ensure-tensor item)))


(defn mget
  "Get an item from an ND object.  If fewer dimensions are
  specified than exist then the return value is a new tensor as a select operation is
  performed."
  ([t x]
  (tech.v3.datatype.base/mget t x))
  ([t x y]
  (tech.v3.datatype.base/mget t x y))
  ([t x y z]
  (tech.v3.datatype.base/mget t x y z))
  ([t x y z & args]
  (apply tech.v3.datatype.base/mget t x y z args)))


(defn mset!
  "Set value(s) on an ND object.  If fewer indexes are provided than dimension then a
  tensor assignment is done and value is expected to be the same shape as the subrect
  of the tensor as indexed by the provided dimensions.  Returns t."
  ([t value]
  (tech.v3.datatype.base/mset! t value))
  ([t x value]
  (tech.v3.datatype.base/mset! t x value))
  ([t x y value]
  (tech.v3.datatype.base/mset! t x y value))
  ([t x y z value]
  (tech.v3.datatype.base/mset! t x y z value))
  ([t x y z w & args]
  (apply tech.v3.datatype.base/mset! t x y z w args)))


(defn native-tensor
  "Create a new native-backed tensor with a :resource-type :auto default
  resource type.

  Options are the same as new-tensor with some additions:

  * `:resource-type` - Defaults to :auto - used as `tech.v3.resource/track track-type`.
  * `:uninitialized?` - Defaults to false - do not 0-initialize the memory."
  ([shape datatype options]
  (tech.v3.tensor-api/native-tensor shape datatype options))
  ([shape datatype]
  (tech.v3.tensor-api/native-tensor shape datatype))
  ([shape]
  (tech.v3.tensor-api/native-tensor shape)))


(defn nd-buffer->buffer-reader
  (^{:tag tech.v3.datatype.Buffer} [b]
  (tech.v3.tensor-api/nd-buffer->buffer-reader b)))


(defn nd-buffer-descriptor->tensor
  "Given a buffer descriptor, produce a tensor"
  ([desc]
  (tech.v3.tensor-api/nd-buffer-descriptor->tensor desc)))


(defn nd-copy!
  "similar to tech.v3.datatype/copy! except this copy is ND aware and
  parallelizes over the outermost dimension.  This useful for compute tensors.
  If you have tensors such as images, see `tensor-copy!`."
  ([src dst]
  (tech.v3.tensor-api/nd-copy! src dst)))


(defn new-tensor
  "Create a new tensor with a given shape.

  Options:

  * `:datatype` - Data of the storage.  Defaults to `:float64`.
  * `:container-type` - Specify the container type of the new tensor.  Defaults to
    `:jvm-heap`.
  * `:resource-type` - One of `tech.v3.resource/track` `:track-type` options.  If allocating
     native tensors, `nil` corresponds to `:gc:`."
  (^{:tag tech.v3.datatype.NDBuffer} [shape & args]
  (apply tech.v3.tensor-api/new-tensor shape args)))


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
  (tech.v3.tensor-api/reduce-axis reduce-fn tensor axis res-dtype))
  ([reduce-fn tensor axis]
  (tech.v3.tensor-api/reduce-axis reduce-fn tensor axis))
  ([reduce-fn tensor]
  (tech.v3.tensor-api/reduce-axis reduce-fn tensor)))


(defn reshape
  "Reshape this item into a new shape.  For this to work, the tensor
  namespace must be required.
  Always returns a tensor."
  (^{:tag tech.v3.datatype.NDBuffer} [t new-shape]
  (tech.v3.datatype.base/reshape t new-shape)))


(defn rotate
  "Rotate dimensions.  Offset-vec must have same count as the rank of t.  Elements of
  that dimension are rotated by the amount specified in the offset vector with 0
  indicating no rotation."
  (^{:tag tech.v3.datatype.NDBuffer} [t offset-vec]
  (tech.v3.datatype.base/rotate t offset-vec)))


(defn rows
  "Return the rows of the tensor in a randomly-addressable structure."
  (^{:tag java.util.List} [src]
  (tech.v3.tensor-api/rows src)))


(defn select
  "Perform a subrect projection or selection
  Shape arguments may be readers, ranges, integers, or the keywords [:all :lla].
  :all means take the entire dimension, :lla means reverse the dimension.
  Arguments are applied left to right and any missing arguments are assumed to
  be :all.

  Example:

  ```clojure
  user> (dtt/select (dtt/->tensor [1 2 3]) [0 2])
  #tech.v3.tensor<object>[2]
  [1 3]

  user> (def tensor (dtt/->tensor
              [[1 2 3]
               [4 5 6]]))
  #'user/tensor
  user> (dtt/select tensor [1] [0 2])
  #tech.v3.tensor<object>[2]
  [4 6]
  user> (dtt/select tensor [0 1] [1 2])
  #tech.v3.tensor<object>[2 2]
  [[2 3]
   [5 6]]
  ```"
  (^{:tag tech.v3.datatype.NDBuffer} [t & args]
  (apply tech.v3.datatype.base/select t args)))


(defn simple-dimensions?
  "Are the dimensions of this object simple meaning read in order with no
  breaks due to striding."
  ([item]
  (tech.v3.tensor-api/simple-dimensions? item)))


(defn slice
  "Slice off Y leftmost dimensions returning a reader of objects.
  If all dimensions are sliced of then the reader reads actual elements,
  else it reads subrect tensors."
  (^{:tag java.util.List} [t n-dims]
  (tech.v3.datatype.base/slice t n-dims)))


(defn slice-right
  "Slice off Y rightmost dimensions returning a reader of objects.
  If all dimensions are sliced of then the reader reads actual elements,
  else it reads subrect tensors."
  (^{:tag java.util.List} [t n-dims]
  (tech.v3.datatype.base/slice-right t n-dims)))


(defn tensor->buffer
  "Get the buffer from a tensor."
  ([item]
  (tech.v3.tensor-api/tensor->buffer item)))


(defn tensor->dimensions
  "Get the dimensions object from a tensor."
  ([item]
  (tech.v3.tensor-api/tensor->dimensions item)))


(defn tensor-copy!
  "Specialized copy with optimized pathways for when tensors have regions of contiguous
  data.  As an example consider a sub-image of a larger image.  Each row can be copied
  contiguously into a new image but there are gaps between them."
  ([src dst options]
  (tech.v3.tensor-api/tensor-copy! src dst options))
  ([src dst]
  (tech.v3.tensor-api/tensor-copy! src dst)))


(defn tensor?
  "Returns true if this implements the tech.v3.datatype.NDBuffer interface."
  ([item]
  (tech.v3.tensor-api/tensor? item)))


(defn transpose
  "In-place transpose an n-d object into a new shape.  Returns a tensor.

  reorder-indexes are the relative indexes of the old indexes

  Example:

```clojure
user> (def tensor (dtt/->tensor (partition 2 (partition 3 (flatten (repeat 6 [:r :g :b]))))))
#'user/tensor
user> tensor
#tech.v3.tensor<object>[3 2 3]
[[[:r :g :b]
  [:r :g :b]]
 [[:r :g :b]
  [:r :g :b]]
 [[:r :g :b]
  [:r :g :b]]]
user> (dtt/transpose tensor [2 0 1])
#tech.v3.tensor<object>[3 3 2]
[[[:r :r]
  [:r :r]
  [:r :r]]
 [[:g :g]
  [:g :g]
  [:g :g]]
 [[:b :b]
  [:b :b]
  [:b :b]]]
user> (dtt/transpose tensor [1 2 0])
#tech.v3.tensor<object>[2 3 3]
[[[:r :r :r]
  [:g :g :g]
  [:b :b :b]]
 [[:r :r :r]
  [:g :g :g]
  [:b :b :b]]]
```"
  (^{:tag tech.v3.datatype.NDBuffer} [t reorder-indexes]
  (tech.v3.datatype.base/transpose t reorder-indexes)))


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
  `(tech.v3.tensor-api/typed-compute-tensor ~datatype ~advertised-datatype ~rank ~shape ~op-code-args ~op-code))
  ([advertised-datatype rank shape op-code-args op-code]
  `(tech.v3.tensor-api/typed-compute-tensor ~advertised-datatype ~rank ~shape ~op-code-args ~op-code))
  ([advertised-datatype shape op-code-args op-code]
  `(tech.v3.tensor-api/typed-compute-tensor ~advertised-datatype ~shape ~op-code-args ~op-code)))


