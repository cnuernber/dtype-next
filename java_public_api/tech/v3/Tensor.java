package tech.v3;


import clojure.lang.IFn;
import static tech.v3.Clj.*;
import static tech.v3.DType.*;
import java.util.List;
import java.util.Map;
import tech.v3.datatype.NDBuffer;


/**
 * <p>The Tensor api exposes a simple and efficient NDBuffer implementation with of zero-copy
 * access to many numeric systems such as Julia, Numpy, and Clojure's Neanderthal linear
 * algebraic system.  The primary interface for this library is tech.v3.datatype.NDBuffer.</p>
 *
 * <p>Given a tech.v3.DType.toBuffer-capable object such as a primitive array or a contiguous
 * block of native memory, a 'reshape' call returns an NDBuffer or you can copy arbitrary data
 * into a container with 'makeTensor'.</p>
 *
 * <p>You can mutate data with the mset api function or directly via the NDBuffer interface
 * methods.</p>
 *
 * <p>Any time a tensor is linearized it is read row-major.  So if you call any base DType
 * functions on it such as 'toDoubleArray' you will get the data in row-major form.  If this
 * tensors is already using a double array buffer such as if you just reshaped a double array
 * it will simply return its buffer.</p>
 *
 * <p>Once you have a tensor then there are a few methods of interacting with it.  All of these
 * methods create views:</p>
 *
 * <ul>
 *   <li><b>select</b> - select a region or reorder within a dimension.</li>
 *   <li><b>transpose</b> - reorder dimensions themselves.</li>
 *   <li><b>reshape</b> - reshape the source buffer into a different NDBuffer.</li>
 *   <li><b>broadcast</b> - enlarge the NDBuffer by duplicating dimensions.</li>
 *   <li><b>slice(Right)</b> - slice a fixed number of dimensions from the left or right of the
 *   tensor returning a list of sub-tensors.  Slicing all dimensions is equivalent to
 *   calling Dtype.toBuffer() meaning you will just get a flat Buffer implementation in
 *   row-major form.</li>
 * </ul>
 *
 * <p>The C ABI of the tensor system is a map with at least specific keys in it.  Necessary keys
 * are clojure keywords:</p>
 *
 * <ul>
 *   <li><b>:ptr</b> - Long integer ptr to data.</li>
 *   <li><b>:elemwise-datatype</b> - One of the numeric datatypes.</li>
 *   <li><b>:endianness</b> - One of ':little-endian' or ':big-endian'.</li>
 *   <li><b>:shape</b> - Integer or Long array of dimensions.</li>
 *   <li><b>:strides</b> - Integer or Long array of byte strides.</li>
 * </ul>
 *
 * <p>Example - Note there are extra keys in the descriptor map.  This is fine; extra keys
 * are ignored:</p>
 * <pre>
 *  try (AutoCloseable ac = stackResourceContext()) {
 *    Map bufDesc = ensureNDBufferDescriptor(reshape(toDoubleArray(range(9)), vector(3,3)));
 *    System.out.println(bufDesc.toString());
 *    System.out.println(NDBufferDescriptorToTensor(bufDesc).toString());
 *  } catch(Exception e){
 *    System.out.println(e.toString());
 *    e.printStackTrace(System.out);
 *  }
 * //Outputs-
 * {:ptr 140054473349344, :datatype :tensor, :elemwise-datatype :float64, :endianness :little-endian, :shape [3 3], :strides [24 8], :native-buffer #native-buffer@0x00007F60F92218E0<float64>[9]
 * [0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000]}
 * #tech.v3.tensor<float64>[3 3]
 * [[0.000 1.000 2.000]
 * [3.000 4.000 5.000]
 * [6.000 7.000 8.000]]
 * </pre>
 *
 * <p>The entire resource descriptor is as the native buffer's gc object so anything else
 * referenced in the metadata will survive as long as the native buffer is referenced.</p>
 */
public class Tensor {

  //Unnecessary
  private Tensor() {}

  static final IFn makeTensorFn = (IFn)requiringResolve("tech.v3.tensor", "->tensor");
  static final IFn asTensorFn = (IFn)requiringResolve("tech.v3.tensor", "as-tensor");
  static final IFn ensureTensorFn = (IFn)requiringResolve("tech.v3.tensor", "ensure-tensor");
  static final IFn applyFn = (IFn)requiringResolve("clojure.core", "apply");
  static final IFn mget = (IFn)requiringResolve("tech.v3.tensor", "mget");
  static final IFn mset = (IFn)requiringResolve("tech.v3.tensor", "mset!");
  static final IFn bcastFn = (IFn)requiringResolve("tech.v3.tensor", "broadcast");
  static final IFn reshapeFn = (IFn)requiringResolve("tech.v3.tensor", "reshape");
  static final IFn selectFn = (IFn)requiringResolve("tech.v3.tensor", "select");
  static final IFn transposeFn = (IFn)requiringResolve("tech.v3.tensor", "transpose");
  static final IFn sliceFn = (IFn)requiringResolve("tech.v3.tensor", "slice");
  static final IFn sliceRightFn = (IFn)requiringResolve("tech.v3.tensor", "slice-right");
  static final IFn computeTensFn = (IFn)requiringResolve("tech.v3.tensor", "compute-tensor");
  static final IFn ensureNDBFn = (IFn)requiringResolve("tech.v3.tensor", "ensure-nd-buffer-descriptor");
  static final IFn NDBToTensFn = (IFn)requiringResolve("tech.v3.tensor", "nd-buffer-descriptor->tensor");



  /**
   * Make an tensor by copying data into a tensor of a given shape and datatype.
   */
  public static NDBuffer makeTensor(Object data, Object shape, Object datatype) {
    Object tens = call(makeTensorFn, data, hashmap(kw("datatype"), datatype));
    return (NDBuffer)call(reshapeFn, tens, shape);
  }
  /**
   * Make an tensor by copying data into a tensor of a given shape, datatype and
   * container type.
   */
  public static NDBuffer makeTensor(Object data, Object shape, Object datatype,
				    Object containerType) {
    Object tens = call(makeTensorFn, data, hashmap(kw("datatype"), datatype,
						   kw("container-type"), containerType));
    return (NDBuffer)call(reshapeFn, tens, shape);
  }
  /**
   * Make a virtualized tensor that will on-demand call indexFn for each element.
   * A 2D tensor will call the 2-arity invoke overload with Long arguments.  The result
   * will be unceremoniously forced into the datatype indicated by datatype.
   */
  public static NDBuffer computeTensor(Object datatype, Object shape, IFn indexFn) {
    //My original argument order here is bad!!
    return (NDBuffer)call(computeTensFn, shape, indexFn, datatype);
  }
  /**
   * Attempt an in-place conversion of the data such as a numpy array into a tensor.
   */
  public static NDBuffer asTensor(Object data) {
    return (NDBuffer)call(asTensorFn, data);
  }
  /**
   * Return the value of the tensor at the leftmost dimension at location dim.
   */
  public static Object mget(NDBuffer tens, long dim) {
    return call(mget, tens, dim);
  }
  /**
   * Return the value of the tensor at the provided dimensions.
   */
  public static Object mget(NDBuffer tens, long dim1, long dim2) {
    return call(mget, tens, dim1, dim2);
  }
  /**
   * Return the value of the tensor at the provided dimensions.
   */
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3) {
    return call(mget, tens, dim1, dim2, dim3);
  }
  /**
   * Return the value of the tensor at the provided dimensions.
   */
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3, long dim4) {
    return call(mget, tens, dim1, dim2, dim3, dim4);
  }
  /**
   * Set the tensor to a value.  If this value is a constant, the tensor will be
   * constant value.  Else val must have same dimensions as tens.
   */
  public static Object mset(NDBuffer tens, Object val) {
    return call(mset, tens, val);
  }
  /**
   * Set the sub-tensor at dim to value val.  val must either be constant
   * or have the same dimensions as the sub-tensor.
   */
  public static Object mset(NDBuffer tens, long dim, Object val) {
    return call(mget, tens, dim, val);
  }
  /**
   * Set the sub-tensor at [dim1,dim2] to value val.  val must either be constant
   * or have the same dimensions as the sub-tensor.
   */
  public static Object mget(NDBuffer tens, long dim1, long dim2, Object val) {
    return call(mget, tens, dim1, dim2, val);
  }
  /**
   * Set the sub-tensor at [dim1,dim2,dim3] to value val.  val must either be constant
   * or have the same dimensions as the sub-tensor.
   */
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3, Object val) {
    return call(mget, tens, dim1, dim2, dim3, val);
  }
  /**
   * Set the sub-tensor at [dim1,dim2,dim3,dim4] to value val.  val must either be constant
   * or have the same dimensions as the sub-tensor.
   */
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3, long dim4,
			    Object val) {
    return call(mget, tens, dim1, dim2, dim3, dim4, val);
  }
  /**
   * Enlarge the tensor by duplicating dimenions.  newShape's dimensions must all be
   * identical or evenly divisible by the shape of src.
   */
  public static NDBuffer broadcast(Object src, Object newShape) {
    return (NDBuffer)call(bcastFn, src, newShape);
  }
  /**
   * Reshape a tensor by first getting it's row-major internal representation and
   * then applying a new shape to it.  New shape need to use every element in
   * src.
   */
  public static NDBuffer reshape(Object src, Object newShape) {
    return (NDBuffer)call(reshapeFn, src, newShape);
  }
  /**
   * Select a subrect and potentially reindex a tensor.  Starting from the left
   * if dim is a number then that exact position is select.  If dim is convertible
   * to a buffer then that dimension is reindexed by the given elements.  The keyword
   * :all may be used to indicate use entire dimension.  Any dimensions that aren't
   * included in the selection are left unchanged.
   */
  public static NDBuffer select(NDBuffer src, Object... dims) {
    return (NDBuffer)call(applyFn, selectFn, src, dims);
  }
  /**
   * Transpose an NDBuffer by reordering its dimensions.  For example given an
   * image of shape [Y X C] we can make a planar representation by moving the the
   * channels first corresponds to reindexing dimensions [2 0 1].
   */
  public static NDBuffer transpose(NDBuffer src, Object dimIndexes) {
    return (NDBuffer)call(transposeFn, src, dimIndexes);
  }
  /**
   * Slice a tensor returning a list of sub-tensors.  Slicing an image of shape [Y X C] by 1
   * returns a list of [X C] tensors.  Slicing by 2 returns a list of [C] tensors and
   * slicing by 3 returns the tensor as a linear buffer.
   */
  public static List slice(NDBuffer src, long ndims) {
    return (List)call(sliceFn, src, ndims);
  }
  /**
   * Slice a tensor logically from the right.  Given an RGB image of shape [Y X C] slicing
   * right by 1 returns a list of length 3 of [Y X] tensors - the red plane followed by the
   * green plane and finally the blue plane.
   */
  public static List sliceRight(NDBuffer src, long ndims) {
    return (List)call(sliceRightFn, src, ndims);
  }
  /**
   * Return the rows.  Equivalent to slice(src, 1).
   */
  public static List rows(NDBuffer src) {
    return (List)call(sliceFn, src, 1);
  }
  /**
   * Return the columns.  Equivalent to sliceRight(src, 1).
   */
  public static List columns(NDBuffer src) {
    return (List)call(sliceRightFn, src, 1);
  }
  /**
   * Get an NDBufferDescription from a tensor.  In the case where the tensors underlying
   * representation is on the jvm-heap this will copy the data into a new native-heap
   * representation.
   */
  public static Map ensureNDBufferDescriptor(Object src) {
    return (Map)call(ensureNDBFn, src);
  }
  /**
   * Given an ndbufferdescriptor create a tensor.  The tensor will reference the buffer
   * descriptor in its metadata.
   */
  public static NDBuffer NDBufferDescriptorToTensor(Map ndBufferDesc) {
    return (NDBuffer)call(NDBToTensFn, ndBufferDesc);
  }

}
