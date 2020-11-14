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
}
