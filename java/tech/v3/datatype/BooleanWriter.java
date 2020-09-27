package tech.v3.datatype;


public interface BooleanWriter extends BooleanBuffer
{
  default boolean read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
