package tech.v3.datatype;


public interface PrimitiveReader extends PrimitiveIO
{
  default void writeBoolean(long idx, boolean val) { throw new UnsupportedOperationException(); }
  default void writeByte(long idx, byte val) { throw new UnsupportedOperationException(); }
  default void writeShort(long idx, short val) { throw new UnsupportedOperationException(); }
  default void writeChar(long idx, char val) { throw new UnsupportedOperationException(); }
  default void writeInt(long idx, int val) { throw new UnsupportedOperationException(); }
  default void writeLong(long idx, long val) { throw new UnsupportedOperationException(); }
  default void writeFloat(long idx, float val) { throw new UnsupportedOperationException(); }
  default void writeDouble(long idx, double val) { throw new UnsupportedOperationException(); }
  default void writeObject(long idx, Object val) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return true; }
  default boolean allowsWrite() { return false; }
}
