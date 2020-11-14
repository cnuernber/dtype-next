package tech.v3.datatype;


import clojure.lang.Keyword;


public interface DoubleTensorReader extends NDBuffer {
  default Object elemwiseDatatype() { return Keyword.intern(null, "float64"); }
  default boolean ndReadBoolean(long idx) {
    return ndReadDouble(idx) != 0.0;
  }
  default boolean ndReadBoolean(long y, long x) {
    return ndReadDouble(y,x) != 0.0;
  }
  default boolean ndReadBoolean(long y, long x, long c) {
    return ndReadDouble(y,x,c) != 0.0;
  }
  default long ndReadLong(long idx) {
    return (long)ndReadDouble(idx);
  }
  default long ndReadLong(long row, long col) {
    return (long)ndReadDouble(row,col);
  }
  default long ndReadLong(long height, long width, long chan) {
    return (long)ndReadDouble(height,width,chan);
  }
  default Object ndReadObject(long c) {
    if (1 != rank()) {
      throw new RuntimeException("Tensor is not rank 1");
    }
    return ndReadDouble(c);
  }
  default Object ndReadObject(long y, long x) {
    if (2 != rank()) {
      throw new RuntimeException("Tensor is not rank 2");
    }
    return ndReadDouble(y,x);
  }
  default Object ndReadObject(long y, long x, long c) {
    if (3 != rank()) {
      throw new RuntimeException("Tensor is not rank 3");
    }
    return ndReadDouble(y,x,c);
  }
}
