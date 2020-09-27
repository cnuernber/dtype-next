package tech.v3.datatype;


public interface ObjectReader extends ObjectBuffer
{
  default void write(long idx, Object value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
