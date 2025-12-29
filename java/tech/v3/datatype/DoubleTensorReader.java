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

  default Object ndReadObject(long idx) {
    return rank() == 1 ? ndReadDouble(idx) : NDBuffer.super.ndReadObject(idx);
  }
  default Object ndReadObject(long y, long x) {
    return rank() == 2 ? ndReadDouble(y, x) : NDBuffer.super.ndReadObject(y, x);
  }
  default Object ndReadObject(long y, long x, long c) {
    return rank() == 3 ? ndReadDouble(y, x, c) : NDBuffer.super.ndReadObject(y, x, c);
  }
  default Object ndReadObject(long w, long y, long x, long c) {
    return rank() == 4 ? ndReadDouble(w, y, x, c) : NDBuffer.super.ndReadObject(w, y, x, c);
  }
}
