package tech.v3.datatype;


public interface PrimitiveReader
{
  boolean readBoolean(long idx);
  byte readByte(long idx);
  short readShort(long idx);
  int readInt(long idx);
  long readLong(long idx);
  float readFloat(long idx);
  double readDouble(long idx);
};
