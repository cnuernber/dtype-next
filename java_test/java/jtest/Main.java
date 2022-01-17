package jtest;

import tech.v3.datatype.IFnDef;
import tech.v3.datatype.LongReader;
import tech.v3.datatype.DoubleReader;
import static tech.v3.Clj.*;
import static tech.v3.DType.*;
import static tech.v3.Tensor.*;
import java.util.ArrayList;
import clojure.lang.RT;
import clojure.lang.IFn;
import java.util.Map;

public class Main
{
  public static void main(String[] args) {
    IFn fntest = new IFnDef() {
	public Object invoke(Object lhs, Object rhs) {
	  return RT.longCast(lhs) + RT.longCast(rhs);
	}
      };

    LongReader buf = new LongReader() {
	public long lsize() { return 3; }
	public long readLong(long idx) { return idx; }
      };

    DoubleReader dbuf = new DoubleReader() {
	public long lsize() { return 3; }
	public double readDouble(long idx) { return (double)idx; }
      };

    ArrayList<Object> data = new ArrayList<Object>();
    data.addAll(buf);
    data.addAll(dbuf);
    System.out.println("Readers: " + data.toString());
    System.out.println("Last Elem: " + call(buf, -1).toString()
		       + " " + call(dbuf,-1).toString()
		       + " " + apply(buf, -1).toString());
    require("tech.v3.datatype");
    IFn reshape = (IFn)requiringResolve("tech.v3.tensor", "reshape");
    int[] ddata = new int[] {0,1,2,3,4,5,6,7,8,9};
    System.out.println(call(reshape, ddata, vector(3,3)).toString());
    System.out.println(opts("one", 1, "two", 2
			    , "three", 3)
		       .toString());

    //Ensure stack resource context mapping is working.
    try (AutoCloseable ac = stackResourceContext()) {
      Object nativeBuf = makeContainer(nativeHeap, int8, opts("log-level", keyword("info")),
				       range(10));
      System.out.println(nativeBuf.toString());
    } catch (Exception e) {
      System.out.println("Error!!" + e.toString());
      e.printStackTrace(System.out);
    }
    System.out.println("After stack pop - nativemem should be released");

    Object srcbuf = makeContainer(float32, range(10));
    Object dstbuf = makeContainer(float32, 10);
    copy(srcbuf, dstbuf);
    System.out.println(dstbuf.toString());
    setConstant(dstbuf, 0);
    System.out.println(dstbuf.toString());
    System.out.println(reshape(range(10), vector(3,3)).toString());
    System.out.println(reshape(toDoubleArray(range(9)), vector(3,3)).toString());
    System.out.println(computeTensor(float64, vector(3,3),
				     new IFnDef() {
				       public Object invoke(Object yidx, Object xidx) {
					 return (RT.longCast(yidx) * 3) + RT.longCast(xidx);
				       }
				     } ).toString());
    try (AutoCloseable ac = stackResourceContext()) {
      Map bufDesc = ensureNDBufferDescriptor(reshape(toDoubleArray(range(9)), vector(3,3)));
      System.out.println(bufDesc.toString());
      System.out.println(NDBufferDescriptorToTensor(bufDesc).toString());
    } catch(Exception e){
      System.out.println(e.toString());
      e.printStackTrace(System.out);
    }

  }
}
