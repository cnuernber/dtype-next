package tech.v3.datatype;


public class ByteConversions
{
  public static byte byteFromBytes(byte val) { return val; }
  public static byte byteFromReader(PrimitiveReader reader, long offset) {
    return reader.readByte(0+offset);
  }
  public static void byteToWriter(byte val, PrimitiveWriter writer, long offset){
    writer.writeByte(0+offset, val);
  }
  public static short shortFromBytesLE(byte b1, byte b2) {
    return (short) ((b1 & 0xFF) | ((b2 & 0xFF) << 8));
  }
  public static short shortFromBytesBE(byte b1, byte b2) {
    return shortFromBytesLE( b2, b1 );
  }
  public static short shortFromReaderLE (PrimitiveReader reader, long offset) {
    return shortFromBytesLE(reader.readByte(0+offset), reader.readByte(1+offset));
  }
  public static short shortFromReaderBE (PrimitiveReader reader, long offset) {
    return shortFromBytesLE(reader.readByte(1+offset), reader.readByte(0+offset));
  }
  public static void shortToWriterLE (short value, PrimitiveWriter writer, long offset) {
    writer.writeByte(0+offset, (byte)(value & 0xFF));
    writer.writeByte(1+offset, (byte)((value >> 8) & 0xFF));
  }
  public static void shortToWriterBE (short value, PrimitiveWriter writer, long offset) {
    writer.writeByte(1+offset, (byte)(value & 0xFF));
    writer.writeByte(0+offset, (byte)((value >> 8) & 0xFF));
  }

  public static int intFromBytesLE(byte b1, byte b2, byte b3, byte b4) {
    return (int) ((b1 & 0xFF) |
		  ((b2 & 0xFF) << 8) |
		  ((b3 & 0xFF) << 16) |
		  ((b4 & 0xFF) << 24));
  }
  public static int intFromBytesBE(byte b1, byte b2, byte b3, byte b4) {
    return intFromBytesLE( b4, b3, b2, b1 );
  }
  public static int intFromReaderLE (PrimitiveReader reader, long offset) {
    return intFromBytesLE(reader.readByte(0+offset), reader.readByte(1+offset),
			  reader.readByte(2+offset), reader.readByte(3+offset));
  }
  public static int intFromReaderBE (PrimitiveReader reader, long offset) {
    return intFromBytesLE(reader.readByte(3+offset), reader.readByte(2+offset),
			  reader.readByte(1+offset), reader.readByte(0+offset));
  }
  public static void intToWriterLE (int value, PrimitiveWriter writer, long offset) {
    writer.writeByte(0+offset, (byte)(value & 0xFF));
    writer.writeByte(1+offset, (byte)((value >> 8) & 0xFF));
    writer.writeByte(2+offset, (byte)((value >> 16) & 0xFF));
    writer.writeByte(3+offset, (byte)((value >> 24) & 0xFF));
  }
  public static void intToWriterBE (int value, PrimitiveWriter writer, long offset) {
    writer.writeByte(3+offset, (byte)(value & 0xFF));
    writer.writeByte(2+offset, (byte)((value >> 8) & 0xFF));
    writer.writeByte(1+offset, (byte)((value >> 16) & 0xFF));
    writer.writeByte(0+offset, (byte)((value >> 24) & 0xFF));
  }


  public static long longFromBytesLE(byte b1, byte b2, byte b3, byte b4,
				     byte b5, byte b6, byte b7, byte b8) {
    return (((long)(b1 & 0xFF)) |
	    ((long)(b2 & 0xFF) << 8) |
	    ((long)(b3 & 0xFF) << 16) |
	    ((long)(b4 & 0xFF) << 24) |
	    (((long)(b5 & 0xFF)) << 32) |
	    (((long)(b6 & 0xFF)) << 40) |
	    (((long)(b7 & 0xFF)) << 48) |
	    (((long)(b8 & 0xFF)) << 56));
  }
  public static long longFromBytesBE(byte b1, byte b2, byte b3, byte b4,
				     byte b5, byte b6, byte b7, byte b8)
  { return longFromBytesLE( b8, b7, b6, b5, b4, b3, b2, b1 ); }
  public static long longFromReaderLE (PrimitiveReader reader, long offset) {
    return longFromBytesLE(reader.readByte(0+offset), reader.readByte(1+offset),
			   reader.readByte(2+offset), reader.readByte(3+offset),
			   reader.readByte(4+offset), reader.readByte(5+offset),
			   reader.readByte(6+offset), reader.readByte(7+offset));
  }
  public static long longFromReaderBE (PrimitiveReader reader, long offset) {
    return longFromBytesLE(reader.readByte(7+offset), reader.readByte(6+offset),
			   reader.readByte(5+offset), reader.readByte(4+offset),
			   reader.readByte(3+offset), reader.readByte(2+offset),
			   reader.readByte(1+offset), reader.readByte(0+offset));
  }
  public static void longToWriterLE (long value, PrimitiveWriter writer, long offset) {
    writer.writeByte(0+offset, (byte)(value & 0xFF));
    writer.writeByte(1+offset, (byte)((value >> 8) & 0xFF));
    writer.writeByte(2+offset, (byte)((value >> 16) & 0xFF));
    writer.writeByte(3+offset, (byte)((value >> 24) & 0xFF));
    writer.writeByte(4+offset, (byte)((value >> 32) & 0xFF));
    writer.writeByte(5+offset, (byte)((value >> 40) & 0xFF));
    writer.writeByte(6+offset, (byte)((value >> 48) & 0xFF));
    writer.writeByte(7+offset, (byte)((value >> 56) & 0xFF));
  }
  public static void longToWriterBE (long value, PrimitiveWriter writer, long offset) {
    writer.writeByte(7+offset, (byte)(value & 0xFF));
    writer.writeByte(6+offset, (byte)((value >> 8) & 0xFF));
    writer.writeByte(5+offset, (byte)((value >> 16) & 0xFF));
    writer.writeByte(4+offset, (byte)((value >> 24) & 0xFF));
    writer.writeByte(3+offset, (byte)((value >> 32) & 0xFF));
    writer.writeByte(2+offset, (byte)((value >> 40) & 0xFF));
    writer.writeByte(1+offset, (byte)((value >> 48) & 0xFF));
    writer.writeByte(0+offset, (byte)((value >> 56) & 0xFF));
  }

  public static float floatFromBytesLE(byte b1, byte b2, byte b3, byte b4) {
    return Float.intBitsToFloat(intFromBytesLE(b1,b2,b3,b4));
  }
  public static float floatFromBytesBE(byte b1, byte b2, byte b3, byte b4) {
    return Float.intBitsToFloat(intFromBytesBE(b1,b2,b3,b4));
  }
  public static float floatFromReaderLE (PrimitiveReader reader, long offset) {
    return Float.intBitsToFloat(intFromReaderLE(reader,offset));
  }
  public static float floatFromReaderBE (PrimitiveReader reader,long offset) {
    return Float.intBitsToFloat(intFromReaderBE(reader,offset));
  }
  public static void floatToWriterLE (float value, PrimitiveWriter writer, long offset) {
    intToWriterLE(Float.floatToRawIntBits(value), writer, offset);
  }
  public static void floatToWriterBE (float value, PrimitiveWriter writer, long offset) {
    intToWriterBE(Float.floatToRawIntBits(value), writer, offset);
  }
  public static double doubleFromBytesLE(byte b1, byte b2, byte b3, byte b4,
					 byte b5, byte b6, byte b7, byte b8) {
    return Double.longBitsToDouble(longFromBytesLE(b1,b2,b3,b4,b5,b6,b7,b8));
  }
  public static double doubleFromBytesBE(byte b1, byte b2, byte b3, byte b4,
				     byte b5, byte b6, byte b7, byte b8) {
    return Double.longBitsToDouble(longFromBytesBE(b1,b2,b3,b4,b5,b6,b7,b8));
  }
  public static double doubleFromReaderLE (PrimitiveReader reader, long offset) {
    return Double.longBitsToDouble(longFromReaderLE(reader,offset));
  }
  public static double doubleFromReaderBE (PrimitiveReader reader, long offset) {
    return Double.longBitsToDouble(longFromReaderBE(reader, offset));
  }
  public static void doubleToWriterLE (double value, PrimitiveWriter writer, long offset) {
    longToWriterLE(Double.doubleToRawLongBits(value), writer, offset);
  }
  public static void doubleToWriterBE (double value, PrimitiveWriter writer, long offset) {
    longToWriterBE(Double.doubleToRawLongBits(value), writer, offset);
  }
}
