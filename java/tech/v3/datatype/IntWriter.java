package tech.v3.datatype;


public interface IntWriter extends IntIO
{
  default int read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
