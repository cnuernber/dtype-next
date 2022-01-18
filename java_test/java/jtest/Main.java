package jtest;

import tech.v3.datatype.IFnDef;
import tech.v3.datatype.LongReader;
import tech.v3.datatype.DoubleReader;
import tech.v3.datatype.ArrayBufferData;
import tech.v3.datatype.Buffer;
import tech.v3.datatype.NDBuffer;
import static tech.v3.Clj.*;
import static tech.v3.DType.*;
import static tech.v3.Tensor.*;
import java.util.ArrayList;
import clojure.lang.RT;
import clojure.lang.IFn;
import java.util.Map;
import java.nio.FloatBuffer;

public class Main
{
  public static void main(String[] args) {
    //Example quick IFn definition.
    IFn fntest = new IFnDef() {
	public Object invoke(Object lhs, Object rhs) {
	  return RT.longCast(lhs) + RT.longCast(rhs);
	}
      };

    //Provide an abstract integer buffer.  Overriding elemwiseDatatype allows
    //you to control the datatype the buffer is interpreted as downstream
    LongReader buf = new LongReader() {
	public long lsize() { return 3; }
	public long readLong(long idx) { return idx; }
      };
    //Provide an abstract floating point buffer.  Similar to LongReader, overriding
    //elemwiseDatatype allows you to present either a float or double datatype
    //downstream
    DoubleReader dbuf = new DoubleReader() {
	public long lsize() { return 3; }
	public double readDouble(long idx) { return (double)idx; }
      };

    //Readers are partial implementations of java.util.List. Most collection
    //functions should work for them.  sort, however, is implemented outside the
    //list interface.
    ArrayList<Object> data = new ArrayList<Object>();
    data.addAll(buf);
    data.addAll(dbuf);
    System.out.println("Readers: " + data.toString());
    System.out.println("Last Elem: " + call(buf, -1).toString()
		       + " " + call(dbuf,-1).toString()
		       + " " + apply(buf, -1).toString());
    //Readers: [0, 1, 2, 0.0, 1.0, 2.0]
    //Last Elem: 2 2.0 2

    //Base clojure functions work fine.
    require("tech.v3.datatype");
    //A slightly slower but more robust symbol resolution mechanism.
    IFn reshape = (IFn)requiringResolve("tech.v3.tensor", "reshape");
    //You can reshape any flat array or java.util.List implementation in-place
    //into a tensor.  Arrays-of-arrays need to go through the makeTensor route.
    int[] ddata = new int[] {0,1,2,3,4,5,6,7,8,9};

    System.out.println(call(reshape, ddata, vector(3,3)).toString());
    //#tech.v3.tensor<int64>[3 3]
    //[[0 1 2]
    // [3 4 5]
    // [6 7 8]]


    //Clojure libraries often use maps with keyword keys as optional arguments to functions.
    //opts allows you to construct one of these maps without needing to type the
    //keyword function out repeatedly.
    System.out.println(opts("one", 1, "two", 2, "three", 3).toString());
    //{:one 1, :two 2, :three 3}

    //When allocating native memory, users can control how the memory is reclaimed.  The default
    //is the keyword ':gc' which is the equivalent of keyword("gc") or kw("gc") for short.
    //The means the memory will be reclaimed when the garbage collector notifies us the
    //memory is no longer reachable by the program.
    //Stack-based resource contexts are available in order to ensure the code within the context
    //will release all allocated native memory immediately upon the 'close' of the context.
    try (AutoCloseable ac = stackResourceContext()) {
      Object nativeBuf = makeContainer(nativeHeap, int8, opts("log-level", keyword("info")),
				       range(10));
      System.out.println(nativeBuf.toString());
    } catch (Exception e) {
      System.out.println("Error!!" + e.toString());
      e.printStackTrace(System.out);
    }
    System.out.println("After stack pop - nativemem should be released");
    //Jan 17, 2022 9:05:39 AM clojure.tools.logging$eval3217$fn__3220 invoke
    //INFO: Malloc - 0x00007F37B91C7D90 - 0000000000000010 bytes
    //#native-buffer@0x00007F37B91C7D90<int8>[10]
    //[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    //Jan 17, 2022 9:05:39 AM clojure.tools.logging$eval3217$fn__3220 invoke
    //INFO: Free   - 0x00007F37B91C7D90 - 0000000000000010 bytes
    //After stack pop - nativemem should be released

    //Datatype includes a high performance copy mechanism that will use the highest
    //known performance primitive for the given container types.  For example, two
    //double arrays will use System/arraycopy while a double array and a double
    //native buffer will use Unsafe/copyMemory.  This means that copying data into a
    //nativeHeap container is extremely fast as is copying data between jvmHeap
    //container
    Object srcbuf = makeContainer(float32, range(10));
    Object dstbuf = makeContainer(float32, 10);
    copy(srcbuf, dstbuf);
    System.out.println(dstbuf.toString());
    //#array-buffer<float32>[10]
    //[0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000, 9.000]

    //setConstant is a high performance primitive when possible.
    setConstant(dstbuf, 0);
    System.out.println(dstbuf.toString());
    //#array-buffer<float32>[10]
    //[0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000]

    //reshape allows you to reshape a 1D buffer or an ND buffer interpreted row-major as
    //1D into an ND tensor.
    System.out.println(reshape(range(10), vector(3,3)).toString());
    //#tech.v3.tensor<int64>[3 3]
    //[[0 1 2]
    // [3 4 5]
    // [6 7 8]]

    System.out.println(reshape(toDoubleArray(range(9)), vector(3,3)).toString());
    //#tech.v3.tensor<float64>[3 3]
    //[[0.000 1.000 2.000]
    // [3.000 4.000 5.000]
    // [6.000 7.000 8.000]]
    System.out.println(computeTensor(float64, vector(3,3),
				     new IFnDef() {
				       public Object invoke(Object yidx, Object xidx) {
					 return (RT.longCast(yidx) * 3) + RT.longCast(xidx);
				       }
				     } ).toString());
    //#tech.v3.tensor<float64>[3 3]
    //[[0.000 1.000 2.000]
    // [3.000 4.000 5.000]
    // [6.000 7.000 8.000]]
    try (AutoCloseable ac = stackResourceContext()) {
      Map bufDesc = ensureNDBufferDescriptor(reshape(toDoubleArray(range(9)), vector(3,3)));
      System.out.println(bufDesc.toString());
      //{:ptr 139877299155184, :datatype :tensor, :elemwise-datatype :float64, :endianness :little-endian, :shape [3 3], :strides [24 8], :native-buffer #native-buffer@0x00007F37B8BA70F0<float64>[9]
      //[0.000, 1.000, 2.000, 3.000, 4.000, 5.000, 6.000, 7.000, 8.000]}
      System.out.println(NDBufferDescriptorToTensor(bufDesc).toString());
      //#tech.v3.tensor<float64>[3 3]
      //[[0.000 1.000 2.000]
      // [3.000 4.000 5.000]
      // [6.000 7.000 8.000]]
    } catch(Exception e){
      System.out.println(e.toString());
      e.printStackTrace(System.out);
    }

    //Ensure nio buffers are supported
    FloatBuffer fbuf = FloatBuffer.wrap(toFloatArray(range (9)));
    System.out.println(reshape(fbuf, vector(3,3)).toString());
    //#tech.v3.tensor<float32>[3 3]
    // [[0.000 1.000 2.000]
    //  [3.000 4.000 5.000]
    //  [6.000 7.000 8.000]]
    fbuf = FloatBuffer.allocate(9);
    System.out.println(reshape(fbuf, vector(3,3)).toString());
    //#tech.v3.tensor<float32>[3 3]
    //[[0.000 0.000 0.000]
    // [0.000 0.000 0.000]
    // [0.000 0.000 0.000]]

    //When data is wrapped via a Buffer or a tensor op then
    //as long as you haven't reindexed it or done some other lazy or abstract operation
    //you can get back to the original data.
    double[] testData = toDoubleArray(range(100));
    Buffer wrappedData = toBuffer(testData);
    ArrayBufferData origData = asArrayBuffer(wrappedData);

    System.out.println(String.valueOf(System.identityHashCode(testData))
		       + " " + String.valueOf(System.identityHashCode(origData.arrayData)));


    //This includes select as long as the selection is contiguous and monotonically
    //incrementing by 1.
    NDBuffer tensData = reshape(toDoubleArray(range (27)), vector(3,3,3));
    NDBuffer smallerTensor = select(tensData, range(1,3));
    System.out.println(smallerTensor.toString());
    //Because we use a range the above condition has to be met.
    System.out.println("Array buffer access? " +
		       (String.valueOf(asArrayBuffer(smallerTensor) != null)));
    // #tech.v3.tensor<float64>[2 3 3]
    // [[[9.000 10.00 11.00]
    //   [12.00 13.00 14.00]
    //   [15.00 16.00 17.00]]
    //  [[18.00 19.00 20.00]
    //   [21.00 22.00 23.00]
    //   [24.00 25.00 26.00]]]
    //  Array buffer access? true


    //If we invert the selection thus reversing the outermost dimension
    smallerTensor = select(tensData, range(2,0,-1));
    System.out.println(smallerTensor.toString());
    //We can no longer get a buffer from this tensor.
    System.out.println("Array buffer access? " +
		       (String.valueOf(asArrayBuffer(smallerTensor) != null)));
    // #tech.v3.tensor<float64>[2 3 3]
    // [[[18.00 19.00 20.00]
    //   [21.00 22.00 23.00]
    //   [24.00 25.00 26.00]]
    //  [[9.000 10.00 11.00]
    //   [12.00 13.00 14.00]
    //   [15.00 16.00 17.00]]]
    // Array buffer access? false
  }
}
