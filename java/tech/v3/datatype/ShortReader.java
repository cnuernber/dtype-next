package tech.v3.datatype;


public interface ShortReader extends ShortIO
{
  default void write(long idx, short value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
