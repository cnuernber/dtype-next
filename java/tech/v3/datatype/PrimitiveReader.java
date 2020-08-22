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


public interface PrimitiveReader extends IOBase, Iterable, IFn,
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
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default int size() { return RT.intCast(lsize()); }
  default Object get(int idx) { return readObject(idx); }
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
    return new PrimitiveReaderIter(this);
  }
  default Object invoke(Object arg) {
    return readObject(RT.uncheckedLongCast(arg));
  }
  default Object applyTo(ISeq items) {
    if (1 == items.count()) {
      return invoke(items.first());
    } else {
      //Abstract method error
      return invoke(items.first(), items.next());
    }
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
