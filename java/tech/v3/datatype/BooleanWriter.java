package tech.v3.datatype;


public interface BooleanWriter extends BooleanIO
{
  default boolean read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
