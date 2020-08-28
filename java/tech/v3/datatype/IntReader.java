package tech.v3.datatype;


public interface IntReader extends IntIO
{
  default void write(long idx, byte value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
