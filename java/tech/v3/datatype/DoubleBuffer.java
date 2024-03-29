package tech.v3.datatype;

import java.util.List;
import clojure.lang.Keyword;
import clojure.lang.RT;
import clojure.lang.IFn;
import ham_fisted.Casts;
import ham_fisted.ChunkedList;
import ham_fisted.Transformables;
import ham_fisted.Reductions;
import ham_fisted.IFnDef;



public interface DoubleBuffer extends Buffer
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "float64"); }
  default boolean add(Object o) { addDouble(Casts.doubleCast(o)); return true; }
  default long readLong(long idx) {return Casts.longCast(readDouble(idx));}
  default Object readObject(long idx) {return readDouble(idx);}
  default void writeLong(long idx, long val) {
    writeDouble(idx, (double)val);
  }
  default void writeObject(long idx, Object val) {
    writeDouble(idx, RT.doubleCast(val));
  }
  public static class DoubleSubBuffer extends SubBuffer implements DoubleBuffer {
    public DoubleSubBuffer(DoubleBuffer list, long sidx, long eidx) {
      super(list, sidx, eidx);
    }
    public void fillRange(long startidx, List v) { DoubleBuffer.super.fillRange(startidx+sidx, v); }
    public Object reduce(IFn rfn, Object init) { return DoubleBuffer.super.reduce(rfn, init); }
  }
  default Buffer subBuffer(long sidx, long eidx) {
    ChunkedList.sublistCheck(sidx, eidx, lsize());
    if(sidx == 0 && eidx == lsize()) return this;
    return new DoubleSubBuffer(this, sidx, eidx);
  }
  default void fillRange(long startidx, List v) {
    if(v.isEmpty()) return;
    ChunkedList.checkIndexRange(0, lsize(), startidx, startidx + v.size());
    Reductions.serialReduction(new Reductions.IndexedDoubleAccum(new IFnDef.OLDO() {
	public Object invokePrim(Object acc, long idx, double v) {
	  ((Buffer)acc).writeDouble(idx+startidx, v);
	  return acc;
	}
      }), this, v);
  }
  default Object reduce(final IFn fn, Object init) {
    IFn.ODO rf = Transformables.toDoubleReductionFn(fn);
    final long sz = lsize();
    for (long idx = 0; idx < sz && !RT.isReduced(init); ++idx)
      init = rf.invokePrim(init, readDouble(idx));
    return Reductions.unreduce(init);
  }
}
