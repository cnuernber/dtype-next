package tech.v3.datatype;


public interface LongWriter extends LongIO
{
  default long readLong(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
};
