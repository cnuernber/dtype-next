package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Sequential;
import clojure.lang.Indexed;
import java.util.Iterator;
import java.util.List;
import java.util.RandomAccess;
import java.util.stream.LongStream;
import clojure.lang.RT;
import clojure.lang.ISeq;


public interface LongReader extends IOBase, Iterable, IFn,
				    List, RandomAccess, Sequential,
				    Indexed, PrimitiveReader
{
  long read(long idx);
  default boolean readBoolean(long idx) {return read(idx) != 0;}
  default byte readByte(long idx) {return (byte)read(idx);}
  default short readShort(long idx) {return (short)read(idx);}
  default int readInt(long idx) {return (int)read(idx);}
  default long readLong(long idx) {return read(idx);}
  default float readFloat(long idx) {return (float)read(idx);}
  default double readDouble(long idx) {return (double)read(idx);}
  default Object elemwiseDatatype () { return Keyword.intern(null, "int64"); }
  default int size() { return RT.intCast(lsize()); }
  default Object get(int idx) { return read(idx); }
  default boolean isEmpty() { return lsize() == 0; }
  default Object[] toArray() {
    int nElems = size();
    Object[] data = new Object[nElems];

    for(int idx=0; idx < nElems; ++idx) {
      data[idx] = read(idx);
    }
    return data;
  }
  default Iterator iterator() {
    return new LongReaderIter(this);
  }
  default Object invoke(Object arg) {
    return read(RT.uncheckedLongCast(arg));
  }
  default Object applyTo(ISeq items) {
    if (1 == items.count()) {
      return invoke(items.first());
    } else {
      //Abstract method error
      return invoke(items.first(), items.next());
    }
  }
  default LongStream typedStream() {
    return LongStream.range(0, size()).map(i -> read(i));
  }
  default int count() { return size(); }
  default Object nth(int idx) { return read(idx); }
  default Object nth(int idx, Object notFound) {
    if (idx >= 0 && idx <= size()) {
      return read(idx);
    } else {
      return notFound;
    }
  }
}
