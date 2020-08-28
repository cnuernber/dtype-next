package tech.v3.datatype;


public interface LongReader extends LongIO
{
  default void write(long idx, byte value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
