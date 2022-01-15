package jtest;

import tech.v3.datatype.IFnDef;
import tech.v3.datatype.LongReader;
import tech.v3.datatype.DoubleReader;
import static tech.v3.datatype.Clj.*;
import java.util.ArrayList;
import clojure.lang.RT;
import clojure.lang.IFn;

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
  }
}
