package tech.v3.datatype;


public interface LongReader extends LongBuffer
{
  default void writeLong(long idx, byte value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
