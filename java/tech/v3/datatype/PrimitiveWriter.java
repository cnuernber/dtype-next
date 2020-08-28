package tech.v3.datatype;


public interface PrimitiveWriter extends PrimitiveIO
{
  default boolean readBoolean(long idx) { throw new UnsupportedOperationException(); }
  default byte readByte(long idx)  { throw new UnsupportedOperationException(); }
  default short readShort(long idx)  { throw new UnsupportedOperationException(); }
  default char readChar(long idx) { throw new UnsupportedOperationException(); }
  default int readInt(long idx) { throw new UnsupportedOperationException(); }
  default long readLong(long idx) { throw new UnsupportedOperationException(); }
  default float readFloat(long idx) { throw new UnsupportedOperationException(); }
  default double readDouble(long idx) { throw new UnsupportedOperationException(); }
  default Object readObject(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
  default boolean allowsWrite() { return true; }
}
