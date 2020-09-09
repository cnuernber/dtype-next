package tech.v3.datatype;

public interface DoubleWriter extends DoubleIO
{
  default double readDouble(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
};
