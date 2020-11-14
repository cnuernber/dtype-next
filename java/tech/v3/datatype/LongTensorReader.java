package tech.v3.datatype;


import clojure.lang.Keyword;


public interface LongTensorReader extends NDBuffer {
  default Object elemwiseDatatype() { return Keyword.intern(null, "int64"); }
  default boolean ndReadBoolean(long idx) {
    return ndReadLong(idx) != 0;
  }
  default boolean ndReadBoolean(long y, long x) {
    return ndReadLong(y,x) != 0;
  }
  default boolean ndReadBoolean(long y, long x, long c) {
    return ndReadLong(y,x,c) != 0;
  }
  default double ndReadDouble(long idx) {
    return (double)ndReadLong(idx);
  }
  default double ndReadDouble(long row, long col) {
    return (double)ndReadLong(row,col);
  }
  default double ndReadDouble(long height, long width, long chan) {
    return (double)ndReadLong(height,width,chan);
  }

  //These overloads are dangerous as the ndReadObject methods are
  //expected to return slices if the tensor is of greater rank
  //than the nd method implies.  This is why NDBuffers aren't
  //Buffers.
  default Object ndReadObject(long idx) {
    if (1 != rank()) {
      throw new RuntimeException("Tensor is not rank 1");
    }
    return ndReadLong(idx);
  }
  default Object ndReadObject(long y, long x) {
    if (2 != rank()) {
      throw new RuntimeException("Tensor is not rank 2");
    }
    return ndReadLong(y,x);
  }
  default Object ndReadObject(long y, long x, long c) {
    if (3 != rank()) {
      throw new RuntimeException("Tensor is not rank 3");
    }
    return ndReadLong(y,x,c);
  }
}
