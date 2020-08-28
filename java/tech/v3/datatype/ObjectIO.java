package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.Stream;


public interface ObjectIO extends PrimitiveIO
{
  Object read(long idx);
  void write(long idx, Object value);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default boolean readBoolean(long idx)
  {
    Object obj = read(idx);
    if (obj instanceof Number) {
      return (double)obj != 0.0;
    } else if (obj instanceof Boolean) {
      return (boolean) obj;
    }
    else {
      return obj != null;
    }
  }
  default byte readByte(long idx) {return RT.byteCast(read(idx));}
  default short readShort(long idx) {return RT.shortCast(read(idx));}
  default char readChar(long idx) {return RT.charCast(read(idx));}
  default int readInt(long idx) {return RT.intCast(read(idx));}
  default long readLong(long idx) {return RT.longCast(read(idx));}
  default float readFloat(long idx) {return RT.floatCast(read(idx));}
  default double readDouble(long idx) {return RT.doubleCast(read(idx));}
  default Object readObject(long idx) {return read(idx);}
  default Stream typedStream() { return stream(); }
  default void writeBoolean(long idx, boolean val) {
    write(idx, val);
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
    write(idx, val);
  }
  default void writeFloat(long idx, float val) {
    write(idx, val);
  }
  default void writeDouble(long idx, double val) {
    write(idx, val);
  }
  default void writeObject(long idx, Object val) {
    write(idx, val);
  }
}
