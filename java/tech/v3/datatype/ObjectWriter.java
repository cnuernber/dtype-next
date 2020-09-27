package tech.v3.datatype;


public interface ObjectWriter extends ObjectBuffer
{
  default Object read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
