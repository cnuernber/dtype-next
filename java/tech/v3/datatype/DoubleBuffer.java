package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.DoubleStream;



public interface DoubleBuffer extends Buffer
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "float64"); }
  default boolean readBoolean(long idx) {
    double dval = readDouble(idx);
    return !Double.isNaN(dval) && dval != 0.0;
  }
  default byte readByte(long idx) {return RT.byteCast(readDouble(idx));}
  default short readShort(long idx) {return RT.byteCast(readDouble(idx));}
  default char readChar(long idx) {return RT.charCast(readDouble(idx));}
  default int readInt(long idx) {return RT.intCast(readDouble(idx));}
  default long readLong(long idx) {return RT.longCast(readDouble(idx));}
  default float readFloat(long idx) {return (float)readDouble(idx);}
  default Object readObject(long idx) {return readDouble(idx);}
  default DoubleStream typedStream() {
    return doubleStream();
  }
  default void writeBoolean(long idx, boolean val) {
    writeDouble(idx, (val ? 1.0 : 0.0));
  }
  default void writeByte(long idx, byte val) {
    writeDouble(idx, (double)val);
  }
  default void writeShort(long idx, short val) {
    writeDouble(idx, (double)val);
  }
  default void writeChar(long idx, char val) {
    writeDouble(idx, (double)val);
  }
  default void writeInt(long idx, int val) {
    writeDouble(idx, (double)val);
  }
  default void writeLong(long idx, long val) {
    writeDouble(idx, (double)val);
  }
  default void writeFloat(long idx, float val) {
    writeDouble(idx, val);
  }
  default void writeObject(long idx, Object val) {
    writeDouble(idx, RT.doubleCast(val));
  }
}
