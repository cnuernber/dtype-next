package tech.v3.datatype;


public interface FloatReader extends FloatIO
{
  default void write(long idx, float value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
