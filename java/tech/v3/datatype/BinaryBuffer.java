package tech.v3.datatype;


public interface BinaryBuffer extends ECount
{
  boolean allowsBinaryRead();
  byte readBinByte(long byteOffset);
  short readBinShort(long byteOffset);
  int readBinInt(long byteOffset);
  long readBinLong(long byteOffset);
  float readBinFloat(long byteOffset);
  double readBinDouble(long byteOffset);

  boolean allowsBinaryWrite();
  void writeBinByte(long byteOffset, byte data);
  void writeBinShort(long byteOffset, short data);
  void writeBinInt(long byteOffset, short data);
  void writeBinLong(long byteOffset, long data);
  void writeBinFloat(long byteOffset, float data);
  void writeBinDouble(long byteOffset, double data);
}
