package tech.v3.datatype;

public interface DoubleWriter extends DoubleIO
{
  default double read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
};
