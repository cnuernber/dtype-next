package tech.v3.datatype;


public interface CharReader extends CharIO
{
  char read(long idx);
  default void write(long idx, char value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
