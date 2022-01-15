package tech.v3.datatype;


import clojure.lang.IFn;
import static tech.v3.datatype.Clj.*;
import java.util.List;
import java.util.Map;

public class JApi {

  public static final Object bool = keyword("boolean");
  public static final Object int8 = keyword("int8");
  public static final Object uint8 = keyword("uint8");
  public static final Object int16 = keyword("int16");
  public static final Object uint16 = keyword("uint16");
  public static final Object int32 = keyword("int32");
  public static final Object uint32 = keyword("uint32");
  public static final Object int64 = keyword("int64");
  public static final Object uint64 = keyword("uint64");
  public static final Object float32 = keyword("float32");
  public static final Object float64 = keyword("float64");
  public static final Object jvmHeap = keyword("jvm-heap");
  public static final Object nativeHeap = keyword("native-heap");




  static final IFn makeContainerFn = (IFn)requiringResolve("tech.v3.datatype", "make-container");
  static final IFn cloneFn = (IFn)requiringResolve("tech.v3.datatype", "clone");
  static final IFn toArrayFn = (IFn)requiringResolve("tech.v3.datatype", "->array");
  static final IFn elemwiseDatatypeFn = (IFn)requiringResolve("tech.v3.datatype",
							      "elemwise-datatype");
  static final IFn ecountFn = (IFn)requiringResolve("tech.v3.datatype", "ecount");
  static final IFn shapeFn = (IFn)requiringResolve("tech.v3.datatype", "shape");
  static final IFn makeListFn = (IFn)requiringResolve("tech.v3.datatype", "make-list");
  static final IFn emapFn = (IFn)requiringResolve("tech.v3.datatype", "emap");
  static final IFn applyFn = (IFn)requiringResolve("clojure.core", "apply");

  //resource management
  static final Object stackContextVar = requiringResolve("tech.v3.resource.stack",
							 "*resource-context*");
  static final Object stackBoundVar = requiringResolve("tech.v3.resource.stack",
						       "*bound-resource-context?*");
  static final IFn releaseResourcesFn = (IFn)requiringResolve("tech.v3.resource.stack",
							      "release-current-resources");


  static final IFn optMap = (IFn)requiringResolve("tech.v3.datatype.jvm-map", "opt-map");

  static final IFn setConstantFn = (IFn)requiringResolve("tech.v3.datatype", "set-constant!");
  static final IFn copyFn = (IFn)requiringResolve("tech.v3.datatype", "copy!");
  static final IFn subBufferFn = (IFn)requiringResolve("tech.v3.datatype", "sub-buffer");
  static final IFn toBufferFn = (IFn)requiringResolve("tech.v3.datatype", "->buffer");


  public static Object elemwiseDatatype(Object val) {
    return elemwiseDatatypeFn.invoke(val);
  }
  public static long ecount(Object val) {
    return (long) ecountFn.invoke(val);
  }
  public static List shape(Object val) {
    return (List)shapeFn.invoke(val);
  }
  public static AutoCloseable stackResourceContext() {
    pushThreadBindings(persmap(stackContextVar, atom(list()),
			       stackBoundVar, true));
    return new AutoCloseable() {
      public void close() {
	try {
	  releaseResourcesFn.invoke();
	} finally {
	  popThreadBindings();
	}
      }
    };
  }
  public static Object makeContainer(Object storage, Object dtype, Object options,
				     Object dataOrNElems) {
    return makeContainerFn.invoke(storage, dtype, options, dataOrNElems);
  }
  public static Object makeContainer(Object storage, Object dtype, Object dataOrNElems) {
    return makeContainerFn.invoke(storage, dtype, dataOrNElems);
  }
  public static Object makeContainer(Object dtype, Object dataOrNElems) {
    return makeContainerFn.invoke(dtype, dataOrNElems);
  }
  public static Object makeContainer(Object dataOrNElems) {
    return makeContainerFn.invoke(dataOrNElems);
  }

  public static Object clone(Object data) { return cloneFn.invoke(data); }
  public static Object toArray(Object data) { return toArrayFn.invoke(data); }
  public static Object toArray(Object data, Object dtype) { return toArrayFn.invoke(data,dtype); }

  public static boolean[] toBooleanArray(Object data) {
    return (boolean[]) toArrayFn.invoke(data, bool);
  }

  public static byte[] toByteArray(Object data) {
    return (byte[]) toArrayFn.invoke(data, int8);
  }

  public static short[] toShortArray(Object data) {
    return (short[]) toArrayFn.invoke(data, int16);
  }

  public static int[] toIntArray(Object data) {
    return (int[]) toArrayFn.invoke(data, int32);
  }

  public static long[] toLongArray(Object data) {
    return (long[]) toArrayFn.invoke(data, int64);
  }

  public static float[] toFloatArray(Object data) {
    return (float[]) toArrayFn.invoke(data, float32);
  }

  public static double[] toDoubleArray(Object data) {
    return (double[]) toArrayFn.invoke(data, float64);
  }

  public static Object setConstant(Object item, long offset, long length, Object value) {
    call(setConstantFn, item, offset, length, value);
    return item;
  }
  public static Object setConstant(Object item, long offset, Object value) {
    call(setConstantFn, item, offset, value);
    return item;
  }
  public static Object setConstant(Object item, Object value) {
    call(setConstantFn, item, value);
    return item;
  }

  public static Object copy(Object src, Object dst) {
    call(copyFn, src, dst);
  }

  public static Object subBuffer(Object src, long offset, long length) {
    return call(subBufferFn, src, offset, length);
  }
  public static Object subBuffer(Object src, long offset) {
    return call(subBufferFn, src, offset);
  }

  public static Buffer toBuffer(Object src) {
    if (src instanceof Buffer) {
      return (Buffer)src;
    } else {
      return (Buffer)call(toBufferFn, src);
    }
  }

  public static PrimitiveList makeList(Object dtype) {
    return (PrimitiveList)makeListFn.invoke(dtype);
  }

  public static Object emap(IFn mapFn, Object resDtype, Object...args) {
    switch(args.length) {
    case 1: return emapFn.invoke(mapFn, resDtype, args[0]);
    case 2: return emapFn.invoke(mapFn, resDtype, args[0], args[1]);
    case 3: return emapFn.invoke(mapFn, resDtype, args[0], args[1], args[2]);
    case 4: return emapFn.invoke(mapFn, resDtype, args[0], args[1], args[2], args[3]);
    default: return applyFn.invoke(emapFn, mapFn, resDtype, args);
    }
  }

  public static Map opts(Object...args) {
    return (Map)optMap.invoke(args);
  }


}
