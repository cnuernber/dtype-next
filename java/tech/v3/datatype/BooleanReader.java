package tech.v3.datatype;


public interface BooleanReader extends BooleanIO
{
  default void writeBoolean(long idx, boolean value)
  { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
