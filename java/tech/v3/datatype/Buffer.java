package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.Indexed;
import clojure.lang.IDeref;
import clojure.lang.IFn;
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


public interface Buffer extends DatatypeBase, IMutList
{
  default byte readByte(long idx) { return RT.byteCast(readLong(idx)); }
  default long readLong(long idx) { return Casts.longCast(readObject(idx)); }
  default double readDouble(long idx) { return Casts.doubleCast(readObject(idx)); }
  Object readObject(long idx);
  default void writeByte(long idx, byte v) { writeLong(idx,v); }
  default void writeLong(long idx, long val) { writeObject(idx,val); }
  default void writeDouble(long idx, double val) { writeDouble(idx,val); }
  void writeObject(long idx, Object val);

  default Buffer subBuffer(long sidx, long eidx) {
    return (Buffer)subList(RT.intCast(sidx), RT.intCast(eidx));
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

  default Object reduce(IFn f, Object init) {
    final long  sz = lsize();
    for(long idx = 0; idx < sz && (!RT.isReduced(init)); ++idx) {
      init = f.invoke(init, readObject(idx));
    }
    if (RT.isReduced(init))
      return ((IDeref)init).deref();
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

  default Object doubleReduction(IFn.ODO op, Object init) {
    final long sz = size();
    for(long idx = 0; idx < sz && !RT.isReduced(init); ++idx)
      init = op.invokePrim(init, readDouble(idx));
    return init;
  }

  default Object longReduction(IFn.OLO op, Object init) {
    final long sz = size();
    for(long idx = 0; idx < sz && !RT.isReduced(init); ++idx)
      init = op.invokePrim(init, readLong(idx));
    return init;
  }

  default Object parallelReduction(IFn initValFn, IFn rfn, IFn mergeFn,
				   ParallelOptions options) {
    return Reductions.parallelIndexGroupReduce(new IFnDef.LLO() {
	public Object invokePrim(long sidx, long eidx) {
	  return Reductions.serialReduction(rfn, initValFn.invoke(), subBuffer(sidx, eidx));
	}
      }, lsize(), mergeFn, options);
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
};
