package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.IntStream;


public interface IntIO extends PrimitiveIO
{
  int read(long idx);
  void write(long idx, int value);
  default Object elemwiseDatatype () { return Keyword.intern(null, "int32"); }
  default boolean readBoolean(long idx) {return read(idx) != 0;}
  default byte readByte(long idx) {return RT.byteCast(read(idx));}
  default short readShort(long idx) {return RT.shortCast(read(idx));}
  default char readChar(long idx) {return RT.charCast(read(idx));}
  default int readInt(long idx) {return read(idx);}
  default long readLong(long idx) {return (long)read(idx);}
  default float readFloat(long idx) {return (float)read(idx);}
  default double readDouble(long idx) {return (double)read(idx);}
  default Object readObject(long idx) {return read(idx);}
  default IntStream typedStream() {
    return intStream();
  }
  default void writeBoolean(long idx, boolean val) {
    write(idx, (val ? 1 : 0));
  }
  default void writeByte(long idx, byte val) {
    write(idx, val);
  }
  default void writeShort(long idx, short val) {
    write(idx, val);
  }
  default void writeChar(long idx, char val) {
    write(idx, val);
  }
  default void writeInt(long idx, int val) {
    write(idx, val);
  }
  default void writeLong(long idx, long val) {
    write(idx, RT.intCast(val));
  }
  default void writeFloat(long idx, float val) {
    write(idx, RT.intCast(val));
  }
  default void writeDouble(long idx, double val) {
    write(idx, RT.intCast(val));
  }
  default void writeObject(long idx, Object val) {
    write(idx, RT.intCast(val));
  }
}
