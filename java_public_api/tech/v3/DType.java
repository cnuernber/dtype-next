package tech.v3;

import tech.v3.datatype.*;
import clojure.lang.IFn;
import clojure.lang.RT;
import clojure.lang.Keyword;
import static tech.v3.Clj.*;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;

/**
 * <p>
 * 'dtype-next' exposes a container-based API for dealing with bulk containers of primitive data
 * efficiently and uniformly indepedent of if they have jvm-heap-backed storage or
 * native-heap-backed.  Elemwise access is provided via the 'Buffer' interface while
 * bulk operations such as copying and setting constant value use fast primitives such
 * as arrayCopy, Arrays.fill, memset and memcpy.  Extremely fast copy pathways are provided
 * to copy from jvm heap storage (jvm primitive arrays) to native heap storage - these usually
 * boil down to a single C memcopy call.</p>
 *
 *
 * <p>All the base C numeric datatypes are supported, unsigned and signed integer types
 * from 8 to 64 bits, 32 and 64 bit floating point types.  Contains of unknown type
 * have type ':object', strings have type ':string', etc.  Unsigned integer types
 * are denoted by types such as ':uint32' or ':uint8'.</p>
 *
 *
 * <p>Care has been taken to make creating custom buffers as easy as possible.  Default methods
 * have been provided for nearly all the methods on tech.v3.datatype.Buffer and if you need
 * only to create a read-only buffer which is common if the values are defined by code then
 * there are helper interfaces that define yet more of the defaults.  These helper classes
 * are (in the tech.v3.datatype namespace): BooleanReader, LongReader, DoubleReader, and
 * ObjectReader.  Users implementing these classes need only to provide an implementation
 * of the lsize and readXXX methods XXX denots the datatype.  For example:</p>
 *
 * <pre>
 * return new LongReader() {
 * public long lsize() { return 4; }
 * public long readLong(long idx) { return idx; }
 * };
 * </pre>
 *
 * <p>There are two key types not represented in this file -
 * tech.v3.datatype.native_buffer.NativeBuffer and
 * tech.v3.datatype.array_buffer.ArrayBuffer.  These are the backing stores of nativeHeap and
 * jvmHeap memory, respectively.  They are immutable datastructures, unlike nio buffers, and
 * they support, as best as possible, 64 bit indexing.  It can be useful at times to get
 * a direct reference to them.</p>
 */
public class DType {

  //Unnecessary
  private DType() {}

  /** Boolean keyword datatype. */
  public static final Keyword bool = keyword("boolean");
  /** Signed byte datatype. */
  public static final Keyword int8 = keyword("int8");
  /** Unsigned byte datatype. */
  public static final Keyword uint8 = keyword("uint8");
  /** Signed short datatype. */
  public static final Keyword int16 = keyword("int16");
  /** Unsigned short datatype. */
  public static final Keyword uint16 = keyword("uint16");
  /** Signed int datatype. */
  public static final Keyword int32 = keyword("int32");
  /** Unsigned int datatype. */
  public static final Keyword uint32 = keyword("uint32");
  /** Signed 64 bit integer datatype. */
  public static final Keyword int64 = keyword("int64");
  /** Unsigned 64 bit integer datatype. */
  public static final Keyword uint64 = keyword("uint64");
  /** 32 bit floating point datatype. */
  public static final Keyword float32 = keyword("float32");
  /** 64 bit floating point datatype. */
  public static final Keyword float64 = keyword("float64");
  /** Allocate data on the JVM heap in JVM primitive arrays. */
  public static final Keyword jvmHeap = keyword("jvm-heap");
  /** Allocate data on the native heap e.g. using 'malloc'. */
  public static final Keyword nativeHeap = keyword("native-heap");




  static final IFn makeContainerFn = requiringResolve("tech.v3.datatype", "make-container");
  static final IFn cloneFn = requiringResolve("tech.v3.datatype", "clone");
  static final IFn toArrayFn = requiringResolve("tech.v3.datatype", "->array");
  static final IFn elemwiseDatatypeFn = requiringResolve("tech.v3.datatype",
							      "elemwise-datatype");
  static final IFn ecountFn = requiringResolve("tech.v3.datatype", "ecount");
  static final IFn shapeFn = requiringResolve("tech.v3.datatype", "shape");
  static final IFn makeListFn = requiringResolve("tech.v3.datatype", "make-list");
  static final IFn emapFn = requiringResolve("tech.v3.datatype.emap", "emap");
  static final IFn applyFn = requiringResolve("clojure.core", "apply");

  //resource management
  static final Object stackContextVar = requiringResolve("tech.v3.resource.stack",
							 "*resource-context*");
  static final Object stackBoundVar = requiringResolve("tech.v3.resource.stack",
						       "*bound-resource-context?*");
  static final IFn releaseResourcesFn = requiringResolve("tech.v3.resource.stack",
							      "release-current-resources");
  static final IFn optMap = requiringResolve("tech.v3.datatype.jvm-map", "opt-map");
  static final IFn setConstantFn = requiringResolve("tech.v3.datatype", "set-constant!");
  static final IFn copyFn = requiringResolve("tech.v3.datatype", "copy!");
  static final IFn subBufferFn = requiringResolve("tech.v3.datatype", "sub-buffer");
  static final IFn toBufferFn = requiringResolve("tech.v3.datatype", "->buffer");
  static final IFn wrapAddressFn = requiringResolve("tech.v3.datatype.native-buffer",
							 "wrap-address");
  static final IFn setNativeDtFn = requiringResolve("tech.v3.datatype.native-buffer",
							 "set-native-datatype");
  static final IFn asNativeBufferFn = requiringResolve("tech.v3.datatype",
							    "as-native-buffer-data");
  static final IFn asArrayBufferFn = requiringResolve("tech.v3.datatype",
							   "as-array-buffer-data");
  static final IFn numericByteWidthFn = requiringResolve("tech.v3.datatype.casting",
							    "numeric-byte-width");
  static final IFn indexedBufferFn = requiringResolve("tech.v3.datatype.io-indexed-buffer",
							   "indexed-buffer");
  static final IFn reverseFn = requiringResolve("tech.v3.datatype-api", "reverse");
  static final IFn asNioBufFn = requiringResolve("tech.v3.datatype.nio-buffer",
						 "as-nio-buffer");
  static final IFn indexedMapReduceFn = requiringResolve("tech.v3.parallel.for",
							 "indexed-map-reduce");

  static final IFn toBitmapFn = requiringResolve("tech.v3.datatype.bitmap", "->bitmap");
  static final IFn mapFactoryFn = requiringResolve("tech.v3.datatype", "map-factory");

  /**
   * Extremely efficient parallelism primitive for working through a fixed number
   * of indexes.  This corresponds to an out-of-core reduction across a wide set
   * of indexes followed by an in-core reduction to the final result.  This method
   * uses the ForkJoinPool's common pool by default and if this thread is already
   * running inside the common pool it runs the job in a single threaded mode.
   *
   * It is safe to call this function recurrently as it checks to see if the thread
   * is already in a common pool thread and if so runs the code serially.
   *
   * @param numIters Max iteration size.
   * @param indexedMapFn Function that takes 2 longs, startIndex and groupLen and
   * produces a single result.
   * @param reduceFn fn that takes a more or less lazy sequence of results and combines
   * them or returns them in-place.  For side-effecting loops this could be the Clojure
   * function dorun which simply realizes everything and returns nil.
   * @param options Options map (keyword keys) described below.
   *
   * <p>Options (may be null):</p>
   * <ul>
   *   <li><b>:max-batch-size</b> - Defaults to 64000 to respect safe points and to make the
   *       result sequence more manageable.</li>
   *   <li><b>:fork-join-pool</b> - Fork join pool to use.  Defaults to the common pool.</li>
   * </ul>
   *
   * <p>Example:</p>
   * <pre>
   * double[] doubles = toDoubleArray(range(1000000));
   * double result =
   *  (double)indexedMapReduce(doubles.length,
   *			       new IFnDef() {
   *				 //parallel indexed map start block
   *				 public Object invoke(Object startIdx, Object groupLen) {
   *				   double sum = 0.0;
   *				   //RT.intCast is a checked cast.  This could
   *				   //potentially overflow but then the Clojure runtime would
   *                               //throw an exception and the double array couldn't
   *				   //address the data.
   *				   int sidx = RT.intCast(startIdx);
   *				   //Note max-batch-size keeps the group len from overflowing
   *				   //size of integer.
   *				   int glen = RT.intCast(groupLen);
   *				   for(int idx = 0; idx &lt; glen; ++idx ) {
   *				     sum += doubles[sidx + idx];
   *				   }
   *				   return sum;
   *				 }
   *			       },
   *			       //Reduction function receives the results of the per-thread
   *			       //reduction.
   *			       new IFnDef() {
   *				 public Object invoke(Object data) {
   *				   double sum = 0.0;
   *
   *				   for( Object c: (Iterable)data) {
   *				     sum += (double)c;
   *				   }
   *				   return sum;
   *				 }
   *			       });
   * </pre>
   */
  public static Object indexedMapReduce(long numIters, IFn indexedMapFn, IFn reduceFn,
					Object options) {
    return indexedMapReduceFn.invoke(numIters, indexedMapFn, reduceFn, options);
  }
  /**
   * Extremely efficient parallelism primitive.  See documentation on the 4-arity form of the
   * function.
   */
  public static Object indexedMapReduce(long numIters, IFn indexedMapFn, IFn reduceFn) {
    return indexedMapReduce(numIters, indexedMapFn, reduceFn, null);
  }
  /**
   * Return the datatype contained in the container.  For example a double array has
   * an elemwise-datatype of the Clojure keyword ':float64'.
   */
  public static Object elemwiseDatatype(Object val) {
    return elemwiseDatatypeFn.invoke(val);
  }
  /**
   * Return the number of elements in the container.  For tensors this means the number
   * of elements if the tensor is read elemwise in row-major fashion.
   */
  public static long ecount(Object val) {
    return (long) ecountFn.invoke(val);
  }
  /**
   * Return the shape of the container as a persistent vector.  null has no shape.
   */
  public static List shape(Object val) {
    return (List)shapeFn.invoke(val);
  }
  /**
   * <p>Open a stack-based resource context.  Futher allocations of native-heap memory will
   * be cleaned up when this object is closed.  This is meant to be used within a
   * try-with-resources pattern.</p>
   *
   * Example:
   *
   * <pre>
   * try (AutoCloseable ac = stackResourceContext()) {
   *    Object nativeBuf = makeContainer(nativeHeap, int8, opts("log-level", keyword("info")),
   * 				       range(10));
   *   System.out.println(nativeBuf.toString());
   * } catch (Exception e) {
   *   System.out.println("Error!!" + e.toString());
   *   e.printStackTrace(System.out);
   *  }
   * System.out.println("After stack pop - nativemem should be released");
   * </pre>
   */
  public static AutoCloseable stackResourceContext() {
    pushThreadBindings(hashmap(stackContextVar, atom(list()), stackBoundVar, true));
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
  /**
   * Make a container of data.
   *
   * @param storage - either jvmHeap or nativeHeap.
   *
   * @param dtype - must be a known datatype and if nativeHeap storage is used must be a
   *   numeric or boolean datatype.
   *
   * @param options - a map of Clojure keyword to optional value dependent upon container
   *   type.  For nativeHeap containers there is ':log-level' - one of the Clojure
   *   keywords ':debug', ':trace', ':info'.  This results in allocation and deallocation
   *   being logged.  Another nativeHeap option is ':resource-type' which is one of
   *   ':gc', ':stack', null, or ':auto' and defaults to  ':auto'.  This means that if there
   *   is a stack resource context open then the allocation will be tracked by the nearest
   *   stack resource context else it will be cleaned up when the garbage collector notes the
   *   object is no longer reachable.
   *
   * @param dataOrNElems - either a container of data or an integer number of elements.
   *
   * @return - an Object that has an efficient conversion to a buffer via toBuffer.
   *
   */
  public static Object makeContainer(Object storage, Object dtype, Object options,
				     Object dataOrNElems) {
    return makeContainerFn.invoke(storage, dtype, options, dataOrNElems);
  }
  /**
   * Make a container of data.  See documentation on 4 arity version.
   */
  public static Object makeContainer(Object storage, Object dtype, Object dataOrNElems) {
    return makeContainerFn.invoke(storage, dtype, dataOrNElems);
  }
  /**
   * Make a container of data.  See documentation on 4 arity version.
   */
  public static Object makeContainer(Object dtype, Object dataOrNElems) {
    return makeContainerFn.invoke(dtype, dataOrNElems);
  }
  /**
   * Make a container of data.  See documentation on 4 arity version.  In this version
   * jvmHeap will be used and it will match the datatype of the passed in data.
   */
  public static Object makeContainer(Object dataOrNElems) {
    return makeContainerFn.invoke(dataOrNElems);
  }
  /**
   * Clone a container of data.  This will use the fastest available method to copy
   * the container's data into JVM heap memory.  This is useful to, for example, copy
   * from native containers to containers safe to return from inside a stack resource
   * context.
   */
  public static Object clone(Object data) { return cloneFn.invoke(data); }
  /**
   * Convert data into the most appropriate JVM array for the datatype.
   */
  public static Object toArray(Object data) { return toArrayFn.invoke(data); }
  /**
   * Convert data into an array of the indicated datatype.
   */
  public static Object toArray(Object data, Object dtype) { return toArrayFn.invoke(data,dtype); }
  /**
   * Convert data into a boolean array.  Numbers will be converted according to the normal
   * numeric rules e.g. 0 is false and anything else is true.
   */
  public static boolean[] toBooleanArray(Object data) {
    return (boolean[]) toArrayFn.invoke(bool, data);
  }
  /**
   * Convert data into a byte array.  Data that is out of bounds of a byte will cause
   * a casting exception to be thrown.
   */
  public static byte[] toByteArray(Object data) {
    return (byte[]) toArrayFn.invoke(int8, data);
  }
  /**
   * Convert data into a short array.  Data that is out of bounds of a short will cause
   * a casting exception to be thrown.
   */
  public static short[] toShortArray(Object data) {
    return (short[]) toArrayFn.invoke(int16, data);
  }
  /**
   * Convert data into a integer array.  Data that is out of bounds of a int will cause
   * a casting exception to be thrown.
   */
  public static int[] toIntArray(Object data) {
    return (int[]) toArrayFn.invoke(int32, data);
  }
  /**
   * Convert data into a long array.  Data that is out of bounds of a long will cause
   * a casting exception to be thrown.
   */
  public static long[] toLongArray(Object data) {
    return (long[]) toArrayFn.invoke(int64, data);
  }
  /**
   * Convert data into a long array.  Data that is out of bounds of a float will cause
   * a casting exception to be thrown.
   */
  public static float[] toFloatArray(Object data) {
    return (float[]) toArrayFn.invoke(float32, data);
  }
  /**
   * Convert data into a double array.  Data that is out of bounds of a double will cause
   * a casting exception to be thrown.
   */
  public static double[] toDoubleArray(Object data) {
    return (double[]) toArrayFn.invoke(float64, data);
  }
  /**
   * Set a container to a constant value.  This tends to be an extremely optimized operation.
   * Returns the container.
   */
  public static Object setConstant(Object item, long offset, long length, Object value) {
    call(setConstantFn, item, offset, length, value);
    return item;
  }
  /**
   * Set a container to a constant value.  This tends to be an extremely optimized operation.
   * Returns the container.
   */
  public static Object setConstant(Object item, long offset, Object value) {
    call(setConstantFn, item, offset, value);
    return item;
  }
  /**
   * Set a container to a constant value.  This tends to be an extremely optimized operation.
   * Returns the container.
   */
  public static Object setConstant(Object item, Object value) {
    call(setConstantFn, item, value);
    return item;
  }
  /**
   * Efficiently copy data from a source container into a destination containe returning the
   * destination container.
   */
  public static Object copy(Object src, Object dst) {
    call(copyFn, src, dst);
    return dst;
  }
  /**
   * Create a sub-buffer from a larger buffer.
   */
  public static Object subBuffer(Object src, long offset, long length) {
    return call(subBufferFn, src, offset, length);
  }
  /**
   * Create a sub-buffer from a larger buffer.
   */
  public static Object subBuffer(Object src, long offset) {
    return call(subBufferFn, src, offset);
  }
  /**
   * Convert an object to an implementation of tech.v3.datatype.Buffer.  This is useful
   * to make code doing an operation independent of the type of data passed in.  Conversions
   * are provided for arrays and anything derived from both java.util.List and
   * java.util.RandomAccess.
   */
  public static Buffer toBuffer(Object src) {
    if (src instanceof Buffer) {
      return (Buffer)src;
    } else {
      return (Buffer)call(toBufferFn, src);
    }
  }
  /**
   * Create a new Buffer implementation that indexes into a previous
   * Buffer implementation via the provided indexes.
   */
  public static Buffer indexedBuffer(Object indexes, Object buffer) {
    return (Buffer)call(indexedBufferFn, indexes, buffer);
  }
  /**
   * Boolean cast that respects numeric values.  Numeric values of 0 are false, any other
   * numeric value is true.  Booleans cast to themselves, null casts to false.
   */
  public static boolean boolCast(Object scalarVal) {
    if (scalarVal instanceof Number) {
      return (0.0 == RT.doubleCast(scalarVal));
    } else if (scalarVal instanceof Boolean) {
      return (boolean)scalarVal;
    } else {
      return scalarVal != null;
    }
  }
  /**
   * Reverse an sequence, range or reader.
   * If range, returns a new range.
   * If sequence, uses clojure.core/reverse
   * If reader, returns a new reader that performs an in-place reverse
   */
  public static Object reverse(Object item) {
    return call(reverseFn, item);
  }
  /**
   * Make an efficient appendable datastructure that contains a primitive backing store.
   * This object has fast conversions to buffers, fast copy semantics, and fast append
   * semantics.
   */
  public static Buffer makeList(Object dtype) {
    return (Buffer)makeListFn.invoke(dtype);
  }
  /**
   * Elemwentwise-map a function create a new lazy buffer.  Operations are performed upon
   * indexed access to the returned Buffer.
   */
  public static Buffer emap(IFn mapFn, Object resDtype, Object...args) {
    switch(args.length) {
    case 0: throw new RuntimeException("emap requires at least one argument to map over.");
    case 1: return (Buffer)emapFn.invoke(mapFn, resDtype, args[0]);
    case 2: return (Buffer)emapFn.invoke(mapFn, resDtype, args[0], args[1]);
    case 3: return (Buffer)emapFn.invoke(mapFn, resDtype, args[0], args[1], args[2]);
    case 4: return (Buffer)emapFn.invoke(mapFn, resDtype, args[0], args[1], args[2], args[3]);
    default: return (Buffer)applyFn.invoke(emapFn, mapFn, resDtype, args);
    }
  }
  /**
   * Create a 'options' map which simply means ensuring the keys are keywords.  This is meant
   * to be a quick shorthand method to create a map of keyword to option value where the user
   * can just pass in strings for the keys.
   */
  public static Map opts(Object...args) {
    return (Map)optMap.invoke(args);
  }
  /**
   * Return the numeric byte width of a given datatype so for example int32 returns 4.
   */
  public static long numericByteWidth(Object dtype) {
    return (long)call(numericByteWidthFn, dtype);
  }
  /**
   * <p>Wrap an integer pointer into a buffer.  If the pointer is invalid of the number
   * of bytes is wrong then the most likely outcome is that your program will crash
   * at some point in the future.</p>
   *
   * See the 4-arity version of this function for full documentation.
   *
   * Returns a native buffer.
   */
  public static Object wrapAddress(Object gcObject, long address, long nBytes) {
    return call(wrapAddressFn, address, nBytes, int8, kw("little-endian"), gcObject);
  }
  /**
   * <p>Wrap an integer pointer into a buffer.  If the pointer is invalid of the number
   * of bytes is wrong then the most likely outcome is that your program will crash
   * at some point in the future.</p>
   *
   * <p>Data is assumed to be little endian format.</p>
   *
   * @param gcObject An optional object passed in that the native buffer will reference.  This
   * keeps the gcObject from being cleaned up by gc-based methods until the native-buffer is
   * no longer referencable.
   * @param address Integer address of data.
   * @param nBytes Number of bytes to reference at address.
   * @param dtype Datatype to interpret the data as.  nBytes must be commensurate with
   * the binary size of dtype.
   *
   *
   * Returns a native buffer.
   */
  public static Object wrapAddress(Object gcObject, long address, long nBytes, Object dtype) {
    Object origbuf = wrapAddress(gcObject,address,nBytes);
    return call(setNativeDtFn, origbuf, dtype);
  }
  /**
   * Attempt to get a native buffer from an object such as a tensor or a numpy array.
   *
   * @return an instance of 'tech.v3.datatype.NativeBufferData' or null if an
   * in-place conversion is not possible.
   */
  public static NativeBufferData asNativeBuffer(Object obj) {
    return (NativeBufferData)call(asNativeBufferFn, obj);
  }
  /**
   * Attempt to get a array buffer from an object such as a tensor.
   *
   * @return an instance 'tech.v3.datatype.ArrayBufferData' or null if an in-place
   * conversion is not possible.
   */
  public static ArrayBufferData asArrayBuffer(Object obj) {
    return (ArrayBufferData)call(asArrayBufferFn, obj);
  }
  /**
   *  Attempt an in-place conversion to a nio buffer.  Returns null if the conversion fails.
   */
  public static java.nio.Buffer asNioBuffer(Object obj) {
    return (java.nio.Buffer)call(asNioBufFn, obj);
  }
  /**
   * Create a roaring bitmap from arbitrary data.
   */
  public static RoaringBitmap toBitmap(Object data) {
    return (RoaringBitmap)call(toBitmapFn, data);
  }
  /**
   * Create a new empty roaring bitmap.
   */
  public static RoaringBitmap emptyBitmap() {
    return (RoaringBitmap)call(toBitmapFn);
  }
  /**
   * Return a function taking exactly n-keys arguments that will rapidly construct
   * a new map.
   */
  public static IFn mapFactory(List keys) {
    return (IFn)mapFactoryFn.invoke(keys);
  }
}
