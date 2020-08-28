package tech.v3.datatype;


public interface LongWriter extends LongIO
{
  default long read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; } 
};
