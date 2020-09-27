package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.Stream;


public interface ObjectBuffer extends Buffer
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default boolean readBoolean(long idx)
  {
    Object obj = readObject(idx);
    if (obj instanceof Number) {
      return RT.doubleCast(obj) != 0.0;
    } else if (obj instanceof Boolean) {
      return (boolean) obj;
    }
    else {
      return obj != null;
    }
  }
  default byte readByte(long idx) {return NumericConversions.byteCast(readObject(idx));}
  default short readShort(long idx) {return NumericConversions.shortCast(readObject(idx));}
  default char readChar(long idx) {return RT.charCast(readObject(idx));}
  default int readInt(long idx) {return NumericConversions.intCast(readObject(idx));}
  default long readLong(long idx) {return NumericConversions.longCast(readObject(idx));}
  default float readFloat(long idx) {return NumericConversions.floatCast(readObject(idx));}
  default double readDouble(long idx) {return NumericConversions.doubleCast(readObject(idx));}
  default Stream typedStream() { return stream(); }
  default void writeBoolean(long idx, boolean val) {
    writeObject(idx, val);
  }
  default void writeByte(long idx, byte val) {
    writeObject(idx, val);
  }
  default void writeShort(long idx, short val) {
    writeObject(idx, val);
  }
  default void writeChar(long idx, char val) {
    writeObject(idx, val);
  }
  default void writeInt(long idx, int val) {
    writeObject(idx, val);
  }
  default void writeLong(long idx, long val) {
    writeObject(idx, val);
  }
  default void writeFloat(long idx, float val) {
    writeObject(idx, val);
  }
  default void writeDouble(long idx, double val) {
    writeObject(idx, val);
  }
  default void writeObject(long idx, Object val) {
    writeObject(idx, val);
  }
}
