package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.Indexed;
import clojure.lang.IDeref;
import clojure.lang.IFn;
import clojure.lang.IPersistentMap;
import clojure.lang.Util;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Collection;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;
import ham_fisted.IMutList;
import ham_fisted.Reductions;
import ham_fisted.ForkJoinPatterns;
import ham_fisted.ParallelOptions;
import ham_fisted.Casts;
import ham_fisted.IFnDef;
import ham_fisted.ChunkedList;
import ham_fisted.Casts;


public interface Buffer extends DatatypeBase, IMutList<Object>
{
  default byte readByte(long idx) { return RT.byteCast(readLong(idx)); }
  default long readLong(long idx) { return Casts.longCast(readObject(idx)); }
  default double readDouble(long idx) { return Casts.doubleCast(readObject(idx)); }
  Object readObject(long idx);
  default void writeByte(long idx, byte v) { writeLong(idx,v); }
  default void writeLong(long idx, long val) { writeObject(idx,val); }
  default void writeDouble(long idx, double val) { writeDouble(idx,val); }
  default void writeObject(long idx, Object val) { throw new RuntimeException("Unimplemented"); }

  public static class SubBuffer implements Buffer {
    public final Buffer list;
    public final long sidx;
    public final long eidx;
    public final long nElems;
    public SubBuffer(Buffer list, long ss, long ee) {
      this.list = list;
      sidx = ss;
      eidx = ee;
      nElems = ee - ss;
    }
    public boolean allowsRead() { return list.allowsRead(); }
    public boolean allowsWrite() { return list.allowsWrite(); }
    public Object elemwiseDatatype() { return list.elemwiseDatatype(); }
    public long lsize() { return nElems; }
    public byte readByte(long idx) { return list.readByte(idx+sidx); }
    public long readLong(long idx) { return list.readLong(idx+sidx); }
    public double readDouble(long idx) { return list.readDouble(idx+sidx); }
    public Object readObject(long idx) { return list.readObject(idx+sidx); }
    public void writeByte(long idx, byte v) { list.writeByte(idx+sidx,v); }
    public void writeLong(long idx, long val) { list.writeLong(idx+sidx,val); }
    public void writeDouble(long idx, double val) { list.writeDouble(idx+sidx,val); }
    public void writeObject(long idx, Object val) { list.writeObject(idx+sidx,val); }
    public void accumPlusLong(long idx, long val) {
      list.accumPlusLong(idx+sidx, val);
    }
    public void accumPlusDouble(long idx, double val) {
      list.accumPlusDouble(idx+sidx, val);
    }
    public Buffer subBuffer(long ssidx, long seidx) {
      ChunkedList.sublistCheck(ssidx, seidx, lsize());
      if(ssidx == 0 && seidx == lsize())
	return this;
      return list.subBuffer(sidx+ssidx, sidx+seidx);
    }
    public IMutList<Object> subList(int sidx, int eidx) {
      return subBuffer(sidx, eidx);
    }
    public Object reduce(IFn rfn, Object init) {
      final Buffer l = list;
      final long ee = eidx;
      if(rfn instanceof IFn.OLO) {
	final IFn.OLO rr = (IFn.OLO)rfn;
	for(long idx = sidx; idx < ee && !RT.isReduced(init); ++idx)
	  init = rr.invokePrim(init, l.readLong(idx));
      }else if (rfn instanceof IFn.ODO) {
	final IFn.ODO rr = (IFn.ODO)rfn;
	for(long idx = sidx; idx < ee && !RT.isReduced(init); ++idx)
	  init = rr.invokePrim(init, l.readDouble(idx));
      } else {
	for(long idx = sidx; idx < ee && !RT.isReduced(init); ++idx)
	  init = rfn.invoke(init, l.readObject(idx));
      }
      return Reductions.unreduce(init);
    }
    public Buffer withMeta(IPersistentMap m) { return ((Buffer)list.withMeta(m)).subBuffer(sidx, eidx); }
    public IPersistentMap meta() { return list.meta(); }
  }

  default Buffer subBuffer(long sidx, long eidx) {
    ChunkedList.sublistCheck(sidx, eidx, lsize());
    if(sidx == 0 && eidx == lsize())
      return this;
    return new SubBuffer(this, sidx, eidx);
  }

  default void accumPlusLong(long idx, long val) {
    writeLong( idx, readLong(idx) + val );
  }
  default void accumPlusDouble(long idx, double val) {
    writeDouble( idx, readDouble(idx) + val );
  }
  default void accPlusLong(int idx, long val) {
    accumPlusLong(idx, val);
  }
  default void accPlusDouble(int idx, double val) {
    accumPlusDouble( idx, val);
  }

  default boolean allowsRead() { return true; }
  default boolean allowsWrite() { return false; }
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default int size() { return RT.intCast(lsize()); }
  default int count() { return size(); }
  default Object get(int idx) { return readObject(idx); }
  default Object set(int idx, Object val) {
    Object current = get(idx);
    writeObject(idx, val);
    return current;
  }
  default long getLong(int idx) { return readLong(idx); }
  default double getDouble(int idx) { return readDouble(idx); }
  default void setLong(int idx, long v) { writeLong(idx, v); }
  default void setDouble(int idx, double v) { writeDouble(idx, v); }
  default Iterator iterator() { return new BufferIter(this); }
  //Ensure reductions happen in the appropriate space.
  default Object reduce(IFn f) {
    final long sz = lsize();
    if (sz == 0 )
      return f.invoke();
    Object init = get(0);
    for(long idx = 1; idx < sz && (!RT.isReduced(init)); ++idx) {
      init = f.invoke(init, readObject(idx));
    }
    if (RT.isReduced(init))
      return ((IDeref)init).deref();
    return init;
  }

  default Object reduce(IFn rfn, Object init) {
    final long ee = lsize();

    if(rfn instanceof IFn.OLO) {
      final IFn.OLO rr = (IFn.OLO)rfn;
      for(long idx = 0; idx < ee && !RT.isReduced(init); ++idx)
	init = rr.invokePrim(init, readLong(idx));
    } else if (rfn instanceof IFn.ODO) {
      final IFn.ODO rr = (IFn.ODO)rfn;
      for(long idx = 0; idx < ee && !RT.isReduced(init); ++idx)
	init = rr.invokePrim(init, readDouble(idx));
    } else {
      for(long idx = 0; idx < ee && !RT.isReduced(init); ++idx)
	init = rfn.invoke(init, readObject(idx));
    }
    return init;
  }

  default Object kvreduce(IFn f, Object init) {
    final long sz = lsize();
    for(long idx = 0; idx < sz && (!RT.isReduced(init)); ++idx) {
      init = f.invoke(init, idx, readObject(idx));
    }
    if (RT.isReduced(init))
      return ((IDeref)init).deref();
    return init;
  }

  default void fillRange(long sidx, long eidx, Object v) {
    ChunkedList.checkIndexRange(0, lsize(), sidx, eidx);
    for(; sidx < eidx; ++sidx)
      writeObject(sidx, v);
  }

  default void fillRange(long startidx, List v) {
    if(v.isEmpty()) return;
    ChunkedList.checkIndexRange(0, lsize(), startidx, startidx + v.size());
    Reductions.serialReduction(new Reductions.IndexedAccum(new IFnDef.OLOO() {
	public Object invokePrim(Object acc, long idx, Object v) {
	  ((Buffer)acc).writeObject(idx+startidx, v);
	  return acc;
	}
      }), this, v);
  }

  default Object parallelReduction(IFn initValFn, IFn rfn, IFn mergeFn,
				   ParallelOptions options) {
    return Reductions.parallelIndexGroupReduce(new IFnDef.LLO() {
	public Object invokePrim(long sidx, long eidx) {
	  return Reductions.serialReduction(rfn, initValFn.invoke(), subBuffer(sidx, eidx));
	}
      }, lsize(), mergeFn, options);
  }

  default Object nth(int idx) {
    long xx = idx;
    if (xx < 0) xx += lsize();
    return readObject(xx);
  }

  default Object nth(int idx, Object notFound) {
    long xx = idx;
    final long sz = lsize();
    if (xx < 0) xx += sz;
    if( xx >= 0 && xx < sz)
      return readObject(xx);
    return notFound;
  }

  default Object invoke(Object idx) {
    long xx = Casts.longCast(idx);
    if (xx < 0) xx += lsize();
    return readObject(xx);
  }

  default Object invoke(Object idx, Object notFound) {
    if(Util.isInteger(idx)) {
      long xx = Casts.longCast(idx);
      final long sz = lsize();
      if (xx < 0) xx += sz;
      if (xx >= 0 && xx < sz)
	return readObject(xx);
    }
    return notFound;
  }

  class BufferSpliterator implements Spliterator {
    private final Buffer list;
    private final long sidx;
    private long eidx;
    long curIdx;

    BufferSpliterator(Buffer list, long sidx, long eidx) {
      this.list = list;
      this.sidx = sidx;
      this.eidx = eidx;
      curIdx = sidx;
    }

    BufferSpliterator(Buffer list) {
      this(list, 0, list.lsize());
    }

    public Spliterator trySplit() {
      final long nsidx = (eidx - sidx) / 2;
      final Spliterator retval = new BufferSpliterator(list, nsidx, eidx);
      eidx = nsidx;
      return retval;
    }

    public long estimateSize() { return eidx - sidx; }

    public boolean tryAdvance(Consumer action) {
      if (action == null)
	throw new NullPointerException();
      final boolean retval = curIdx < eidx;
      if(retval) {
	action.accept(list.readObject(curIdx));
	++curIdx;
      }
      return retval;
    }

    public void forEachRemaining(Consumer c) {
      final long ee = eidx;
      final Buffer ll = list;
      if(c instanceof DoubleConsumer) {
	final DoubleConsumer dc = (DoubleConsumer)c;
	for(long cc = curIdx; cc < ee; ++cc) {
	  dc.accept(ll.readDouble(cc));
	}
	curIdx = eidx;
	return;
      }
      else if (c instanceof LongConsumer) {
	final LongConsumer dc = (LongConsumer)c;
	for(long cc = curIdx; cc < ee; ++cc) {
	  dc.accept(ll.readLong(cc));
	}
	curIdx = eidx;
	return;
      }
      for(long cc = curIdx; cc < ee; ++cc) {
	c.accept(ll.readObject(cc));
      }
      curIdx = eidx;
    }

    public int characteristics() {
      return Spliterator.ORDERED | Spliterator.SIZED
	| Spliterator.SUBSIZED | Spliterator.IMMUTABLE;
    }
  }

  default Spliterator spliterator() {
    return new BufferSpliterator(this);
  }

  default LongStream indexStream(boolean parallel) {
    LongStream retval =  LongStream.range(0, lsize());
    return parallel ? retval.parallel() : retval;
  }

  default Stream objStream(boolean parallel) {
    return indexStream(parallel).mapToObj((long idx)->readObject(idx));
  }

  default DoubleStream doubleStream(boolean parallel) {
    return indexStream(parallel).mapToDouble((long idx)->readDouble(idx));
  }

  default LongStream longStream(boolean parallel) {
    return indexStream(parallel).map((long idx)->readLong(idx));
  }

  default IntStream intStream(boolean parallel) {
    return indexStream(parallel).mapToInt((long idx)->RT.intCast(readLong(idx)));
  }

  public class CopyingReducer implements Buffer {
    public final Buffer src;
    public final Buffer dst;
    public CopyingReducer(Buffer s, Buffer d) {
      src = s;
      dst = d;
    }
    public Object readObject(long sidx) { return src.readObject(sidx); }
    public long lsize() { return src.lsize(); }
    public Buffer subBuffer(long sidx, long eidx) {
      return new CopyingReducer(src.subBuffer(sidx,eidx), dst.subBuffer(sidx,eidx));
    }
    public Object parallelReduction(IFn initValFn, IFn rfn, IFn mergeFn,
				    ParallelOptions options) {
      return Reductions.parallelIndexGroupReduce(new IFnDef.LLO() {
	  public Object invokePrim(long sidx, long eidx) {
	    dst.fillRange(sidx, src.subBuffer(sidx, eidx));
	    return initValFn.invoke();
	  }
	}, lsize(), mergeFn, options);
    }
    public Object reduce(IFn rfn, Object acc) {
      dst.fillRange(0, src);
      return acc;
    }
  }
};
