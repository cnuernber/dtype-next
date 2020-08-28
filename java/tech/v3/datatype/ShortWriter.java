package tech.v3.datatype;


public interface ShortWriter extends ShortIO
{
  default short read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
