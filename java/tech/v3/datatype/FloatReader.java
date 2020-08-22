package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.stream.DoubleStream;


public interface FloatReader extends PrimitiveReader
{
  float read(long idx);
  default boolean readBoolean(long idx) {return read(idx) != 0;}
  default byte readByte(long idx) {return (byte)read(idx);}
  default short readShort(long idx) {return (short)read(idx);}
  default char readChar(long idx) {return (char)read(idx);}
  default int readInt(long idx) {return (int)read(idx);}
  default long readLong(long idx) {return (long)read(idx);}
  default float readFloat(long idx) {return read(idx);}
  default double readDouble(long idx) {return (double)read(idx);}
  default Object readObject(long idx) {return read(idx);}
  default Object elemwiseDatatype () { return Keyword.intern(null, "float32"); }
  default DoubleStream typedStream() {
    return doubleStream();
  }
}
