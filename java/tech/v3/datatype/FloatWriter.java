package tech.v3.datatype;


public interface FloatWriter extends FloatIO
{
  default void write(long idx, float value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
