package tech.v3.datatype;


public interface ByteWriter extends ByteIO
{
  default byte read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
