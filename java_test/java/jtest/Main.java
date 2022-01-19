package jtest;

import tech.v3.datatype.IFnDef;
import tech.v3.datatype.LongReader;
import tech.v3.datatype.DoubleReader;
import tech.v3.datatype.ArrayBufferData;
import tech.v3.datatype.NativeBufferData;
import tech.v3.datatype.Buffer;
import tech.v3.datatype.NDBuffer;
import static tech.v3.Clj.*;
import static tech.v3.DType.*;
import static tech.v3.Tensor.*;
import tech.v3.DType; //explicit access to clone method
import java.util.ArrayList;
import clojure.lang.RT;
import clojure.lang.IFn;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
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
    IFn reshape = requiringResolve("tech.v3.tensor", "reshape");
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

    //When allocating native memory, users can control how the memory is reclaimed.
    //The default is the keyword ':gc' which is the equivalent of keyword("gc") or
    //kw("gc") for short.  The means the memory will be reclaimed when the garbage
    //collector notifies us the memory is no longer reachable by the program.
    //Stack-based resource contexts are available in order to ensure the code within
    //the context will release all allocated native memory immediately upon the
    //'close' of the context.
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

    //makeTensor copies data into the tensor.
    System.out.println(makeTensor(range(9), vector(3,3), float32).toString());
    //#tech.v3.tensor<float32>[3 3]
    // [[0.000 1.000 2.000]
    //  [3.000 4.000 5.000]
    //  [6.000 7.000 8.000]]
    //We can also make tensor's based on native-heap memory.
    System.out.println(makeTensor(range(9), vector(3,3), float32, nativeHeap).toString());
    //#tech.v3.tensor<float32>[3 3]
    // [[0.000 1.000 2.000]
    //  [3.000 4.000 5.000]
    //  [6.000 7.000 8.000]]

    //Native memory tensors have an in-place conversion to a native buffer.
    NDBuffer ntens = makeTensor(range(9), vector(3,3), float32, nativeHeap);
    NativeBufferData ndata = asNativeBuffer(ntens);
    System.out.println(ndata.address);
    //140397742494704



    //Neanderthal is a high speed linear algebra system with bindings to MKL, openCL, and
    //cuda.
    //Overall Neanderthal is *very* well documented both with educational styloe
    //books and with API documention.

    //See Neaderthal's main website:
    //https://neanderthal.uncomplicate.org/
    //along with detailed API documentation
    //https://neanderthal.uncomplicate.org/codox/
    require("uncomplicate.neanderthal.core");
    IFn denseConstructor = requiringResolve("uncomplicate.neanderthal.native", "dge");
    Object denseMatrix = call(denseConstructor, 3, 3, range(9));
    System.out.println(denseMatrix.toString());
    // #RealGEMatrix[double, mxn:3x3, layout:column, offset:0]

    //Neaderthal support for dtype-next requires you to require a namespace.  Then
    //protocols are auto-loaded and neaderthal dense matrixes will have a as-tensor pathway.
    require("tech.v3.libs.neanderthal");
    //in-place conversion into dtype-next land is then supported.
    NDBuffer ndBuf = asTensor(denseMatrix);
    //Neaderthal defaults to column-major storage.
    System.out.println(ndBuf.toString());
    //#tech.v3.tensor<float64>[3 3]
    // [[0.000 3.000 6.000]
    //  [1.000 4.000 7.000]
    //  [2.000 5.000 8.000]]

    //We can create row-major matrix by passing in an option map.
    Object denseMatrixRowMajor = call(denseConstructor, 3, 3, range(9),
				      opts("layout", kw("row")));

    ndBuf = asTensor(denseMatrixRowMajor);
    System.out.println(ndBuf.toString());
    //#tech.v3.tensor<float64>[3 3]
    //[[0.000 1.000 2.000]
    // [3.000 4.000 5.000]
    // [6.000 7.000 8.000]]

    //Neanderthal objects require an explicit release step.  Potentially in a wrapper
    //we could bind this to auto-closeable and try with resources.
    Object releaseFn = requiringResolve("uncomplicate.commons.core", "release");
    call(releaseFn, denseMatrix);
    call(releaseFn, denseMatrixRowMajor);



    //tech.ml.dataset is a in-memory column-store data system similar to pandas
    //or R's dplyr.  It has an extensive API - https://techascent.github.io/tech.ml.dataset/
    Object datasetConstructor = requiringResolve("tech.v3.dataset", "->dataset");
    //Default construction support for csv, tsv, xls, xlsx, sequence-of-maps and map-of-columns
    //is included.
    Object ds = call(datasetConstructor, "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv");
    //Datasets print nicely regardless of size.  We may change print to print begin..end but
    //regardless you can always print a dataset safely without bombing your print system.
    //Datasets print in markdown table format so you can paste them directly into a markdown
    //document if you want a table.
    System.out.println(ds.toString());
    //https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [560 3]:

    //| symbol |       date | price |
    //|--------|------------|------:|
    //|   MSFT | 2000-01-01 | 39.81 |
    //|   MSFT | 2000-02-01 | 36.35 |
    //|   MSFT | 2000-03-01 | 43.22 |
    //|   MSFT | 2000-04-01 | 28.37 |
    //|   MSFT | 2000-05-01 | 25.45 |
    // ...

    //Datasets implement a subset of java.util.Map - they are maps where the colnames are
    //the keys and the vals are the columns.  The maps are functional, however, so 'put' will
    //throw an exception.  Use clojure.core's assoc method to create a new dataset.
    Map dsMap = (Map)ds;

    //Columns aren't themselves readers specifically but they do implement IFn expecting
    //indexes and they also print nicely.
    Object prices = dsMap.get("price");
    System.out.println(prices);
    //#tech.v3.dataset.column<float64>[560]
    //price
    //[39.81, 36.35, 43.22, 28.37, 25.45, 32.54, 28.40, 28.40, 24.53, 28.02, 23.34, 17.65, 24.84, 24.00, 22.25, 27.56, 28.14, 29.70, 26.93, 23.21...]

    //Print the last price
    System.out.println("Last price: " + call(prices, -1).toString());
    //Last price: 223.02

    //Datasets support a few java.time objects - Instants, LocalDates, and LocalTimes.  These
    //are stored 'packed' meaning they are stored as their primitive integer representation.
    //Instances are stored as 64bit microseconds past epoch.  LocalDates are stored as 32bit
    //epoch-days and local-times are stores as 64bit microseconds past midnight.
    //These specific datatypes are prefixed with :packed-.
    Object dates = dsMap.get("date");
    System.out.println("Packed date datatype: " + elemwiseDatatype(dates).toString());
    //Packed date datatype: :packed-local-date

    //Reading from this column's readObject member or its IFn interface will get you back
    //a LocalDate
    System.out.println(call(dates, 0).toString());


    //Datatype contains a namespace with math functions that are designed to work
    //lazily with readers. Here we create a price-squared column
    //Datasets specifically are a version of Clojure's persistent map system so
    //Clojure's assoc can be used to add new columns.
    //And finally often we use 'head' to make printing nice.
    Object sqFn = requiringResolve("tech.v3.datatype.functional", "sq");
    Object headFn = requiringResolve("tech.v3.dataset", "head");
    Object newDs = assoc(ds, "price^2", call(sqFn, prices));
    System.out.println(call(headFn, newDs).toString());
    //https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv [5 4]:

    //| symbol |       date | price |   price^2 |
    //|--------|------------|------:|----------:|
    //|   MSFT | 2000-01-01 | 39.81 | 1584.8361 |
    //|   MSFT | 2000-02-01 | 36.35 | 1321.3225 |
    //|   MSFT | 2000-03-01 | 43.22 | 1867.9684 |
    //|   MSFT | 2000-04-01 | 28.37 |  804.8569 |
    //|   MSFT | 2000-05-01 | 25.45 |  647.7025 |


    //You can get the rows of the dataset in 'map' form where the keys are shared
    //between all the maps and each get operations reads data from the backing store.

    Object rowsFn = requiringResolve("tech.v3.dataset", "rows");

    //The rows object implements Buffer which means it implements both  IFn and
    //java.util.List so you can use whichever.
    //The easiest pathway IMHO is to use the IFn or Buffer readObject interface as then
    //you get negative from-the-end indexing.
    Object rows = (Object)call(rowsFn, newDs);
    System.out.println(call(rows, 0).toString());
    // {"date" #object[java.time.LocalDate 0xe360e01 "2000-01-01"], "symbol" "MSFT", "price^2" 1584.8361000000002, "price" 39.81}

    System.out.println(call(rows,-1).toString());
    // {"date" #object[java.time.LocalDate 0x210588 "2010-03-01"], "symbol" "AAPL", "price^2" 49737.9204, "price" 223.02}

    //Saving/loading to arrow is done through a specific namespace.
    //Documentation - https://techascent.github.io/tech.ml.dataset/tech.v3.libs.arrow.html
    Object writeFn = requiringResolve("tech.v3.libs.arrow", "dataset->stream!");
    call(writeFn, newDs, "test.arrow");
    //Lots of annoying debug logging output.

    //Now you can mmap that back unless you are on an m-1 mac.  For m-1 macs you need to
    //run JDK-17 and load the memory module-
    //"--add-modules" "jdk.incubator.foreign,jdk.incubator.vector"
    //"--enable-native-access=ALL-UNNAMED"
    Object readFn = requiringResolve("tech.v3.libs.arrow", "stream->dataset");
    //Consider using a resource context here-
    try (AutoCloseable ac = stackResourceContext()) {
      //If open-type isn't provided it uses the much more robust input-stream
      //pathway.
      Object mmapds = call(readFn, "test.arrow", opts("open-type", kw("mmap")));
      System.out.println(call(headFn, mmapds).toString());
      //test.arrow [5 4]:
      //| symbol |       date | price |   price^2 |
      //|--------|------------|------:|----------:|
      //|   MSFT | 2000-01-01 | 39.81 | 1584.8361 |
      //|   MSFT | 2000-02-01 | 36.35 | 1321.3225 |
      //|   MSFT | 2000-03-01 | 43.22 | 1867.9684 |
      //|   MSFT | 2000-04-01 | 28.37 |  804.8569 |
      //|   MSFT | 2000-05-01 | 25.45 |  647.7025 |



      //Cloning a datasets copies it completely and efficiently into jvm memory
      //So it can safely escape the resource context.  In general clone copies the object
      //as efficiently as possible into jvmHeap memory.
      //Cloning also serves to realize any outstanding lazy operations.
      Map cloneds = (Map)DType.clone(mmapds);
      System.out.println(call(headFn, cloneds).toString());
      //test.arrow [5 4]:
      //| symbol |       date | price |   price^2 |
      //|--------|------------|------:|----------:|
      //|   MSFT | 2000-01-01 | 39.81 | 1584.8361 |
      //|   MSFT | 2000-02-01 | 36.35 | 1321.3225 |
      //|   MSFT | 2000-03-01 | 43.22 | 1867.9684 |
      //|   MSFT | 2000-04-01 | 28.37 |  804.8569 |
      //|   MSFT | 2000-05-01 | 25.45 |  647.7025 |
    } catch(Exception e){
      System.out.println(e.toString());
      e.printStackTrace(System.out);
    }


    //Datasets can be constructed from maps of columns.
    HashMap dsInput = new HashMap();
    dsInput.put("doubles", toDoubleArray(range(10)));
    dsInput.put("shorts", toShortArray(range(9, -1, -1)));
    System.out.println(call(datasetConstructor, dsInput).toString());
    //_unnamed [10 2]:
    //| doubles | shorts |
    //|--------:|-------:|
    //|     0.0 |      9 |
    //|     1.0 |      8 |
    //|     2.0 |      7 |
    //|     3.0 |      6 |
    //|     4.0 |      5 |
    //|     5.0 |      4 |
    //|     6.0 |      3 |
    //|     7.0 |      2 |
    //|     8.0 |      1 |
    //|     9.0 |      0 |

    //There are a lot of crucial dataset concepts not covered here -
    //missing, filtering, sorting, grouping, selecting/duplicate a subset of
    //rows.  Please see:
    //https://techascent.github.io/tech.ml.dataset/quick-reference.html
    //https://techascent.github.io/tech.ml.dataset/walkthrough.html

    //DType also includes a high performance parallelism primitive named
    //indexed-map-reduce.  This primitive iterates in-order through many indexes
    //in a block and then reduces the result of those blocks.  Block size is dicated
    //by options - see function documentation.
    //This iteration strategy allows the per-thread iteration mechanism to keep temporary
    //variables on the stack as opposed to in objects.  Stack-based variables are much much
    //more likely to be picked up by hotspot (or any compiler) and vectorized than object
    //variables.
    double[] doubles = toDoubleArray(range(1000000));
    double result =
      (double)indexedMapReduce(doubles.length,
			       new IFnDef() {
				 //parallel indexed map start block
				 public Object invoke(Object startIdx, Object groupLen) {
				   double sum = 0.0;
				   //RT.intCast is a checked cast.  This could
				   //potentially overflow but then the double array can't
				   //address the data.
				   int sidx = RT.intCast(startIdx);
				   //Note max-batch-size keeps the group len from overflowing
				   //size of integer.
				   int glen = RT.intCast(groupLen);
				   for(int idx = 0; idx < glen; ++idx ) {
				     sum += doubles[sidx + idx];
				   }
				   return sum;
				 }
			       },
			       //Reduction function receives the results of the per-thread
			       //reduction.
			       new IFnDef() {
				 public Object invoke(Object data) {
				   double sum = 0.0;

				   for( Object c: (Iterable)data) {
				     sum += (double)c;
				   }
				   return sum;
				 }
			       });
    System.out.println("Reduction result: " + String.valueOf(result));
    //Reduction result: 4.999995E11

    //Now that was a long form super high efficiency summation.  Now we trade a bit of
    //efficiency for robustness in two forms, first we automatically filter out
    //NaN values and second we use Kahan's compensated summation algorithm to give the
    //accumulator more effective bits than 64.

    System.out.println("Built in reduction result: " +
		       String.valueOf(call(requiringResolve("tech.v3.datatype.statistics",
							    "sum"),
					   doubles)));
    //Built in reduction result: 4.999995E11



    //Neanderthal boots up Clojure's agent pool which means that when it comes time to
    //shutdown you need to call shutdown-agents else you get a nice 1 minute hang
    //on shutdown.  This is always safe to call regardless.
    call(requiringResolve("clojure.core", "shutdown-agents"));
  }
}
