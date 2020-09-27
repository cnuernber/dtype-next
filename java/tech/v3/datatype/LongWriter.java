package tech.v3.datatype;


public interface LongWriter extends LongBuffer
{
  default long readLong(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
};
