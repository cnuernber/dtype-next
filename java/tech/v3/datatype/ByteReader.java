package tech.v3.datatype;


public interface ByteReader extends ByteIO
{
  default void write(long idx, byte value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
