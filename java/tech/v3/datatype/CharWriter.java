package tech.v3.datatype;


public interface CharWriter extends CharIO
{
  default char read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
