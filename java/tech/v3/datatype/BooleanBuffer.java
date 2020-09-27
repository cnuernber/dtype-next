package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.IntStream;


public interface BooleanBuffer extends Buffer
{
  default byte readByte(long idx) {return (byte) (readBoolean(idx) ? 1 : 0);}
  default short readShort(long idx) {return (short) (readBoolean(idx) ? 1 : 0);}
  default char readChar(long idx) {return (char) (readBoolean(idx) ? 1 : 0);}
  default int readInt(long idx) {return (int) (readBoolean(idx) ? 1 : 0);}
  default long readLong(long idx) {return (long) (readBoolean(idx) ? 1 : 0);}
  default float readFloat(long idx) {return (float) (readBoolean(idx) ? 1 : 0);}
  default double readDouble(long idx) {return (double) (readBoolean(idx) ? 1 : 0);}
  default Object readObject(long idx) {return readBoolean(idx);}
  default void writeByte(long idx, byte val) {
    writeBoolean(idx, val != 0 ? true : false);
  }
  default void writeShort(long idx, short val) {
    writeBoolean(idx, val != 0 ? true : false);
  }
  default void writeChar(long idx, char val) {
    writeBoolean(idx, val != 0 ? true : false);
  }
  default void writeInt(long idx, int val) {
    writeBoolean(idx, val != 0 ? true : false);
  }
  default void writeLong(long idx, long val) {
    writeBoolean(idx, val != 0 ? true : false);
  }
  default void writeFloat(long idx, float val) {
    writeBoolean(idx, val != 0.0f ? true : false);
  }
  default void writeDouble(long idx, double val) {
    writeBoolean(idx, val != 0.0 ? true : false);
  }
  default void writeObject(long idx, Object val) {
    if (val instanceof Number)
      writeBoolean(idx, RT.doubleCast(val) != 0.0 ? true : false);
    else if (val instanceof Boolean)
      writeBoolean(idx, (boolean)val);
    else
      writeBoolean(idx, val != null);
  }
  default Object elemwiseDatatype () {return Keyword.intern(null, "boolean");}
  default IntStream typedStream() {
    return intStream();
  }
}
