package tech.v3.datatype;


import clojure.lang.Keyword;


public interface ObjectTensorReader extends NDBuffer {
  default Object elemwiseDatatype() { return Keyword.intern(null, "float64"); }
  default boolean ndReadBoolean(long idx) {
    return BooleanConversions.from(ndReadObject(idx));
  }
  default boolean ndReadBoolean(long y, long x) {
    return BooleanConversions.from(ndReadObject(y,x));
  }
  default boolean ndReadBoolean(long y, long x, long c) {
    return BooleanConversions.from(ndReadObject(y,x,c));
  }
  default long ndReadLong(long idx) {
    return NumericConversions.longCast(ndReadObject(idx));
  }
  default long ndReadLong(long row, long col) {
    return NumericConversions.longCast(ndReadObject(row,col));
  }
  default long ndReadLong(long height, long width, long chan) {
    return NumericConversions.longCast(ndReadObject(height,width,chan));
  }
  default double ndReadDouble(long idx) {
    return NumericConversions.doubleCast(ndReadObject(idx));
  }
  default double ndReadDouble(long row, long col) {
    return NumericConversions.doubleCast(ndReadObject(row,col));
  }
  default double ndReadDouble(long height, long width, long chan) {
    return NumericConversions.doubleCast(ndReadObject(height,width,chan));
  }
}
