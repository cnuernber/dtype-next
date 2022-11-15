package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.IFn;
import ham_fisted.Casts;
import ham_fisted.ChunkedList;


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
    public Object reduce(IFn rfn, Object init) { return LongBuffer.super.reduce(rfn, init); }
    public Object doubleReduction(IFn.ODO rfn, Object init) {
      return LongBuffer.super.doubleReduction(rfn, init);
    }
  }

  default Buffer subBuffer(long sidx, long eidx) {
    ChunkedList.sublistCheck(sidx, eidx, lsize());
    if(sidx == 0 && eidx == lsize()) return this;
    return new LongSubBuffer(this, sidx, eidx);
  }
  default Object reduce(final IFn fn, Object init) {
    final IFn.OLO rrfn = fn instanceof IFn.OLO ? (IFn.OLO)fn : new IFn.OLO() {
	public Object invokePrim(Object lhs, long v) {
	  return fn.invoke(lhs, v);
	}
      };
    return longReduction(rrfn, init);
  }
  default Object doubleReduction(IFn.ODO fn, Object init) {
    return longReduction(new IFn.OLO() {
	public Object invokePrim(Object lhs, long v) {
	  return fn.invokePrim(lhs, (double)v);
	}
      }, init);
  }
}
