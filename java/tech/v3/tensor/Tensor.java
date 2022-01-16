package tech.v3.tensor;


import clojure.lang.IFn;
import static tech.v3.datatype.Clj.*;
import static tech.v3.datatype.DType.*;
import java.util.List;
import java.util.Map;
import tech.v3.datatype.NDBuffer;


public class Tensor {

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




  public static NDBuffer makeTensor(Object data, Object shape, Object datatype) {
    Object tens = call(makeTensorFn, data, hashmap(kw("datatype"), datatype));
    return (NDBuffer)call(reshapeFn, tens, shape);
  }
  public static NDBuffer makeTensor(Object data, Object shape, Object datatype,
				    Object containerType) {
    Object tens = call(makeTensorFn, data, hashmap(kw("datatype"), datatype,
						   kw("container-type"), containerType));
    return (NDBuffer)call(reshapeFn, tens, shape);
  }
  public static NDBuffer computeTensor(Object datatype, Object shape, IFn indexFn) {
    //My original argument order here is bad!!
    return (NDBuffer)call(computeTensFn, shape, indexFn, datatype);
  }
  public static NDBuffer asTensor(Object data) {
    return (NDBuffer)call(asTensorFn, data);
  }
  public static Object mget(NDBuffer tens, long dim) {
    return call(mget, tens, dim);
  }
  public static Object mget(NDBuffer tens, long dim1, long dim2) {
    return call(mget, tens, dim1, dim2);
  }
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3) {
    return call(mget, tens, dim1, dim2, dim3);
  }
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3, long dim4) {
    return call(mget, tens, dim1, dim2, dim3, dim4);
  }

  public static Object mset(NDBuffer tens, Object val) {
    return call(mset, tens, val);
  }
  public static Object mset(NDBuffer tens, long dim, Object val) {
    return call(mget, tens, dim, val);
  }
  public static Object mget(NDBuffer tens, long dim1, long dim2, Object val) {
    return call(mget, tens, dim1, dim2, val);
  }
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3, Object val) {
    return call(mget, tens, dim1, dim2, dim3, val);
  }
  public static Object mget(NDBuffer tens, long dim1, long dim2, long dim3, long dim4,
			    Object val) {
    return call(mget, tens, dim1, dim2, dim3, dim4, val);
  }

  public static NDBuffer broadcast(Object src, Object newShape) {
    return (NDBuffer)call(bcastFn, src, newShape);
  }

  public static NDBuffer reshape(Object src, Object newShape) {
    return (NDBuffer)call(reshapeFn, src, newShape);
  }

  public static NDBuffer select(NDBuffer src, Object... dims) {
    return (NDBuffer)call(applyFn, selectFn, src, dims);
  }

  public static NDBuffer transpose(NDBuffer src, Object dimIndexes) {
    return (NDBuffer)call(transposeFn, src, dimIndexes);
  }

  public static List slice(NDBuffer src, long ndims) {
    return (List)call(sliceFn, src, ndims);
  }

  public static List sliceRight(NDBuffer src, long ndims) {
    return (List)call(sliceRightFn, src, ndims);
  }

  public static List rows(NDBuffer src) {
    return (List)call(sliceFn, src, 1);
  }
  public static List columns(NDBuffer src) {
    return (List)call(sliceRightFn, src, 1);
  }

}
