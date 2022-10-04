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

  default double doubleReduction(DoubleBinaryOperator op, double init) {
    final long sz = size();
    for(long idx = 0; idx < sz; ++idx)
      init = op.applyAsDouble(init, readDouble(idx));
    return init;
  }

  default long longReduction(LongBinaryOperator op, long init) {
    final long sz = size();
    for(long idx = 0; idx < sz; ++idx)
      init = op.applyAsLong(init, readLong(idx));
    return init;
  }

  default public void doubleForEach(DoubleConsumer c) {
    final long sz = lsize();
    for (long idx = 0; idx < sz; ++idx)
      c.accept(readDouble(idx));
  }
  default public void longForEach(LongConsumer c) {
    final long sz = size();
    for (long idx = 0; idx < sz; ++idx)
      c.accept(readLong(idx));
  }
  default DoubleStream doubleStream() {
    return StreamSupport.doubleStream(new BufferDoubleSpliterator(this, 0, lsize(), null),false);
  }
  default LongStream longStream() {
    return LongStream.range(0, size()).map(i -> readLong(i));
  }
  default IntStream intStream() {
    return IntStream.range(0, size()).map(i -> readInt(i));
  }
};
