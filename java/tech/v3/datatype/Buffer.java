package tech.v3.datatype;


import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.Indexed;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.stream.Stream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;


public interface PrimitiveIO extends IOBase, Iterable, IFn,
				     List, RandomAccess, Sequential,
				     Indexed
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

  default boolean allowsRead() { return true; }
  default boolean allowsWrite() { return false; }
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default int size() { return RT.intCast(lsize()); }
  default Object get(int idx) { return readObject(idx); }
  default Object set(int idx, Object val) {
    writeObject(idx, val);
    return null;
  }
  default boolean isEmpty() { return lsize() == 0; }
  default Object[] toArray() {
    int nElems = size();
    Object[] data = new Object[nElems];

    for(int idx=0; idx < nElems; ++idx) {
      data[idx] = readObject(idx);
    }
    return data;
  }
  default Iterator iterator() {
    return new PrimitiveIOIter(this);
  }
  default Object invoke(Object arg) {
    return readObject(RT.uncheckedLongCast(arg));
  }
  default Object invoke(Object arg, Object arg2) {
    writeObject(RT.uncheckedLongCast(arg), arg2);
    return null;
  }
  default Object applyTo(ISeq items) {
    if (1 == items.count()) {
      return invoke(items.first());
    } else if (2 == items.count()) {
      //Abstract method error
      return invoke(items.first(), items.next().first());
    }
    else
      throw new RuntimeException("Too many arguments to applyTo");
  }
  default Object nth(int idx) { return readObject(idx); }
  default Object nth(int idx, Object notFound) {
    if (idx >= 0 && idx <= size()) {
      return readObject(idx);
    } else {
      return notFound;
    }
  }
  default DoubleStream doubleStream() {
    return StreamSupport.doubleStream(new RangeDoubleSpliterator(0, size(),
								 new RangeDoubleSpliterator.LongDoubleConverter() { public double longToDouble(long arg) { return readDouble(arg); } },
								 false),
				      false);
  }
  default LongStream longStream() {
    return LongStream.range(0, size()).map(i -> readLong(i));
  }
  default IntStream intStream() {
    return IntStream.range(0, size()).map(i -> readInt(i));
  }
};
