package tech.v3.datatype;


import java.util.List;
import clojure.lang.Keyword;
import clojure.lang.IFn;
import clojure.lang.RT;
import ham_fisted.Casts;
import ham_fisted.ChunkedList;
import ham_fisted.Transformables;
import ham_fisted.Reductions;
import ham_fisted.IFnDef;


public interface LongBuffer extends Buffer
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "int64"); }
  default boolean add(Object o) { addLong(Casts.longCast(o)); return true; }
  default double readDouble(long idx) {return (double)readLong(idx);}
  default Object readObject(long idx) {return readLong(idx);}
  default void writeDouble(long idx, double val) {
    writeLong(idx, Casts.longCast(val));
  }
  default void writeObject(long idx, Object val) {
    writeLong(idx, Casts.longCast(val));
  }

  public static class LongSubBuffer extends SubBuffer implements LongBuffer {
    public LongSubBuffer(LongBuffer list, long sidx, long eidx) {
      super(list, sidx, eidx);
    }
    public void fillRange(long startidx, List v) {
      LongBuffer.super.fillRange(startidx + sidx, v);
    }
    public Object reduce(IFn rfn, Object init) { return LongBuffer.super.reduce(rfn, init); }
  }

  default Buffer subBuffer(long sidx, long eidx) {
    ChunkedList.sublistCheck(sidx, eidx, lsize());
    if(sidx == 0 && eidx == lsize()) return this;
    return new LongSubBuffer(this, sidx, eidx);
  }
  default void fillRange(long startidx, List v) {
    if(v.isEmpty()) return;
    ChunkedList.checkIndexRange(0, lsize(), startidx, startidx + v.size());
    Reductions.serialReduction(new Reductions.IndexedLongAccum(new IFnDef.OLLO() {
	public Object invokePrim(Object acc, long idx, long v) {
	  ((Buffer)acc).writeLong(idx+startidx, v);
	  return acc;
	}
      }), this, v);
  }
  default Object reduce(final IFn fn, Object init) {
    IFn.OLO rf = Transformables.toLongReductionFn(fn);
    final long sz = lsize();
    for (long idx = 0; idx < sz && !RT.isReduced(init); ++idx)
      init = rf.invokePrim(init, readLong(idx));
    return init;
  }
}
