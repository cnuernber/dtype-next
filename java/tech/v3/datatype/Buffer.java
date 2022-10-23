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
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;
import ham_fisted.IMutList;
import ham_fisted.Reductions;
import ham_fisted.ForkJoinPatterns;
import ham_fisted.ParallelOptions;


public interface Buffer extends DatatypeBase, IMutList
{
  boolean readBoolean(long idx);
  byte readByte(long idx);
  short readShort(long idx);
  char readChar(long idx);
  int readInt(long idx);
  long readLong(long idx);
  float readFloat(long idx);
  double readDouble(long idx);
  Object readObject(long idx);
  void writeBoolean(long idx, boolean val);
  void writeByte(long idx, byte val);
  void writeShort(long idx, short val);
  void writeChar(long idx, char val);
  void writeInt(long idx, int val);
  void writeLong(long idx, long val);
  void writeFloat(long idx, float val);
  void writeDouble(long idx, double val);
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
  default boolean getBoolean(int idx) { return readBoolean(idx); }
  default void setLong(int idx, long v) { writeLong(idx, v); }
  default void setDouble(int idx, double v) { writeDouble(idx, v); }
  default void setBoolean(int idx, boolean v) { writeBoolean(idx, v); }
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
    final IFn gfn = new IFnDef() {
	public Object invoke(Object osidx, Object oeidx) {
	  final long sidx = RT.longCast(osidx);
	  final long eidx = RT.longCast(oeidx);
	  return Reductions.serialReduction(rfn, initValFn.invoke(), subBuffer(sidx, eidx));
	}
      };
    final Iterable groups = (Iterable) ForkJoinPatterns.parallelIndexGroups(lsize(), gfn, options);
    final Iterator giter = groups.iterator();
    Object initObj = giter.next();
    while(giter.hasNext())
      initObj = mergeFn.invoke(initObj, giter.next());
    return initObj;
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
    return indexStream(parallel).mapToInt((long idx)->readInt(idx));
  }
};
