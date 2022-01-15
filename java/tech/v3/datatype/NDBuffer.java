package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.Sequential;
import clojure.lang.RT;
import clojure.lang.ISeq;
import clojure.lang.Indexed;

import java.util.ArrayList;
import java.util.List;
import java.util.RandomAccess;

public interface NDBuffer extends DatatypeBase, Iterable, IFnDef,
				  Sequential, Indexed,
				  List, RandomAccess
{
  // Buffer may be nil if this isn't a buffer backed tensor
  default Object buffer() { return null; }
  Object dimensions();
  LongNDReader indexSystem();
  Buffer bufferIO();
  default Iterable shape() { return indexSystem().shape(); }
  //count of shape
  default int rank() { return indexSystem().rank(); }
  //Outermost dimension
  default long outermostDim() { return indexSystem().outermostDim(); }
  default long lsize() { return indexSystem().lsize(); }
  //Scalar read methods have to be exact to the number of dimensions of the
  //tensor.
  boolean ndReadBoolean(long idx);
  boolean ndReadBoolean(long row, long col);
  boolean ndReadBoolean(long height, long width, long chan);
  void ndWriteBoolean(long idx, boolean value);
  void ndWriteBoolean(long row, long col, boolean value);
  void ndWriteBoolean(long height, long width, long chan, boolean value);
  long ndReadLong(long idx);
  long ndReadLong(long row, long col);
  long ndReadLong(long height, long width, long chan);
  void ndWriteLong(long idx, long value);
  void ndWriteLong(long row, long col, long value);
  void ndWriteLong(long height, long width, long chan, long value);
  double ndReadDouble(long idx);
  double ndReadDouble(long row, long col);
  double ndReadDouble(long height, long width, long chan);
  void ndWriteDouble(long idx, double value);
  void ndWriteDouble(long row, long col, double value);
  void ndWriteDouble(long height, long width, long chan, double value);

  // Object read methods can return slices or values.
  Object ndReadObject(long idx);
  Object ndReadObject(long row, long col);
  Object ndReadObject(long height, long width, long chan);
  Object ndReadObjectIter(Iterable dims);
  void ndWriteObject(long idx, Object value);
  void ndWriteObject(long row, long col, Object value);
  void ndWriteObject(long height, long width, long chan, Object value);
  Object ndWriteObjectIter(Iterable dims, Object value);


  default void ndAccumPlusLong(long idx, long value) {
    ndWriteLong(idx, ndReadLong(idx) + value);
  }
  default void ndAccumPlusLong(long row, long col, long value) {
    ndWriteLong(row, col, ndReadLong(row, col) + value);
  }
  default void ndAccumPlusLong(long height, long width, long chan, long value) {
    ndWriteLong(height, width, chan, ndReadLong(height, width, chan) + value);
  }


  default void ndAccumPlusDouble(long idx, double value) {
    ndWriteDouble(idx, ndReadDouble(idx) + value );
  }
  default void ndAccumPlusDouble(long row, long col, double value) {
    ndWriteDouble(row, col, ndReadDouble(row, col) + value);
  }
  default void ndAccumPlusDouble(long height, long width, long chan, double value) {
    ndWriteDouble(height, width, chan, ndReadDouble(height, width, chan) + value);
  }


  default boolean allowsRead() { return true; }
  default boolean allowsWrite() { return false; }
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object arg) {
    return ndReadObject(RT.longCast(arg));
  }
  default Object invoke(Object arg, Object arg2) {
    return ndReadObject(RT.longCast(arg), RT.longCast(arg2));
  }
  default Object invoke(Object arg, Object arg2, Object arg3) {
    return ndReadObject(RT.longCast(arg), RT.longCast(arg2), RT.longCast(arg3));
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4) {
    ArrayList<Object> args = new ArrayList<Object>() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
    } };
    return ndReadObjectIter(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5) {
    ArrayList<Object> args = new ArrayList<Object>() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
    } };
    return ndReadObjectIter(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5, Object arg6) {
    ArrayList<Object> args = new ArrayList<Object>() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
      add(arg6);
    } };
    return ndReadObjectIter(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5, Object arg6, Object arg7) {
    ArrayList<Object> args = new ArrayList<Object>() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
      add(arg6);
      add(arg7);
    } };
    return ndReadObjectIter(args);
  }
  default Object invoke(Object arg, Object arg2, Object arg3, Object arg4,
			Object arg5, Object arg6, Object arg7, Object arg8) {
    ArrayList<Object> args = new ArrayList<Object>() { {
      add(arg);
      add(arg2);
      add(arg3);
      add(arg4);
      add(arg5);
      add(arg6);
      add(arg7);
      add(arg8);
    } };
    return ndReadObjectIter(args);
  }
  default Object applyTo(ISeq items) {
    return ndReadObjectIter((Iterable)items);
  }
  default Object nth(int idx) { return ndReadObject(idx); }
  default Object nth(int idx, Object notFound) {
    if (idx >= 0 && idx <= outermostDim()) {
      return ndReadObject(idx);
    } else {
      return notFound;
    }
  }
  default int size() { return RT.intCast(outermostDim()); }
  default Object get(int idx) { return ndReadObject(idx); }
  default boolean isEmpty() { return size() == 0; }
  default Object[] toArray() {
    int nElems = size();
    Object[] data = new Object[nElems];
    for(int idx=0; idx < nElems; ++idx) {
      data[idx] = ndReadObject(idx);
    }
    return data;
  }
}
