package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.LongStream;



public interface LongIO extends PrimitiveIO
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "int64"); }
  default boolean readBoolean(long idx) {return readLong(idx) != 0;}
  default byte readByte(long idx) {return (byte)readLong(idx);}
  default short readShort(long idx) {return (short)readLong(idx);}
  default char readChar(long idx) {return (char)readLong(idx);}
  default int readInt(long idx) {return (int)readLong(idx);}
  default float readFloat(long idx) {return (float)readLong(idx);}
  default double readDouble(long idx) {return (double)readLong(idx);}
  default Object readObject(long idx) {return readLong(idx);}
  default LongStream typedStream() {
    return longStream();
  }
  default void writeBoolean(long idx, boolean val) {
    writeLong(idx, (val ? 1 : 0));
  }
  default void writeByte(long idx, byte val) {
    writeLong(idx, val);
  }
  default void writeShort(long idx, short val) {
    writeLong(idx, val);
  }
  default void writeChar(long idx, char val) {
    writeLong(idx, val);
  }
  default void writeInt(long idx, int val) {
    writeLong(idx, val);
  }
  default void writeFloat(long idx, float val) {
    writeLong(idx, RT.longCast(val));
  }
  default void writeDouble(long idx, double val) {
    writeLong(idx, RT.longCast(val));
  }
  default void writeObject(long idx, Object val) {
    writeLong(idx, RT.longCast(val));
  }
}
