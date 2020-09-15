package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.Indexed;

import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

public interface PrimitiveNDIO extends IOBase, Iterable, IFn,
				       Sequential, Indexed,
				       List, RandomAccess
{
  Iterable shape();
  int rank(); //count of shape
  //Outermost dimension
  long outermostDim();

  //Scalar read methods have to be exact to the number of dimensions of the
  //tensor.
  boolean readBoolean(long idx);
  boolean readBoolean(long row, long col);
  boolean readBoolean(long height, long width, long chan);
  void writeBoolean(long idx, boolean value);
  void writeBoolean(long row, long col, boolean value);
  void writeBoolean(long height, long width, long chan, boolean value);
  long readLong(long idx);
  long readLong(long row, long col);
  long readLong(long height, long width, long chan);
  void writeLong(long idx, long value);
  void writeLong(long row, long col, long value);
  void writeLong(long height, long width, long chan, long value);
  double readDouble(double idx);
  double readDouble(double row, double col);
  double readDouble(double height, double width, double chan);
  void writeDouble(double idx, double value);
  void writeDouble(double row, double col, double value);
  void writeDouble(double height, double width, double chan, double value);

  // Object read methods can return slices or values.
  Object readObject(Object idx);
  Object readObject(Object row, Object col);
  Object readObject(Object height, Object width, Object chan);
  Object ndReadObject(Iterable dims);
  void writeObject(Object idx, Object value);
  void writeObject(Object row, Object col, Object value);
  void writeObject(Object height, Object width, Object chan, Object value);
  Object ndWriteObject(Iterable dims, Object value);

  default boolean allowsRead() { return true; }
  default boolean allowsWrite() { return false; }
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object arg) {
    return readObject(RT.longCast(arg));
  }
  default Object invoke(Object arg, Object arg2) {
    return readObject(RT.longCast(arg), RT.longCast(arg2));
  }
  default Object invoke(Object arg, Object arg2, Object arg3) {
    return readObject(RT.longCast(arg), RT.longCast(arg2), RT.longCast(arg3));
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4) {
    ArrayList args = new ArrayList() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
    } };
    return ndReadObject(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5) {
    ArrayList args = new ArrayList() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
    } };
    return ndReadObject(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5, Object arg6) {
    ArrayList args = new ArrayList() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
      add(arg6);
    } };
    return ndReadObject(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5, Object arg6, Object arg7) {
    ArrayList args = new ArrayList() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
      add(arg6);
      add(arg7);
    } };
    return ndReadObject(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5, Object arg6, Object arg7, Object arg8) {
    ArrayList args = new ArrayList() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
      add(arg6);
      add(arg7);
      add(arg8);
    } };
    return ndReadObject(args);
  }
  default Object applyTo(ISeq items) {
    return ndReadObject((Iterable)items);
  }
  default Object nth(int idx) { return readObject(idx); }
  default Object nth(int idx, Object notFound) {
    if (idx >= 0 && idx <= outermostDim()) {
      return readObject(idx);
    } else {
      return notFound;
    }
  }
  default int size() { return RT.intCast(outermostDim()); }
  default boolean isEmpty() { return size() == 0; }
  default Object[] toArray() {
    int nElems = size();
    Object[] data = new Object[nElems];

    for(int idx=0; idx < nElems; ++idx) {
      data[idx] = readObject(idx);
    }
    return data;
  }
}
