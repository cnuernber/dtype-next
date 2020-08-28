package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.DoubleStream;



public interface DoubleIO extends PrimitiveIO
{
  double read(long idx);
  void write(long idx, double value);
  default Object elemwiseDatatype () { return Keyword.intern(null, "float64"); }
  default boolean readBoolean(long idx) {return read(idx) != 0.0;}
  default byte readByte(long idx) {return RT.byteCast(read(idx));}
  default short readShort(long idx) {return RT.byteCast(read(idx));}
  default char readChar(long idx) {return RT.charCast(read(idx));}
  default int readInt(long idx) {return RT.intCast(read(idx));}
  default long readLong(long idx) {return RT.longCast(read(idx));}
  default float readFloat(long idx) {return (float)read(idx);}
  default double readDouble(long idx) {return read(idx);}
  default Object readObject(long idx) {return read(idx);}
  default DoubleStream typedStream() {
    return doubleStream();
  }
  default void writeBoolean(long idx, boolean val) {
    write(idx, (val ? 1.0 : 0.0));
  }
  default void writeByte(long idx, byte val) {
    write(idx, (double)val);
  }
  default void writeShort(long idx, short val) {
    write(idx, (double)val);
  }
  default void writeChar(long idx, char val) {
    write(idx, (double)val);
  }
  default void writeInt(long idx, int val) {
    write(idx, (double)val);
  }
  default void writeLong(long idx, long val) {
    write(idx, (double)val);
  }
  default void writeFloat(long idx, float val) {
    write(idx, val);
  }
  default void writeDouble(long idx, double val) {
    write(idx, val);
  }
  default void writeObject(long idx, Object val) {
    write(idx, RT.doubleCast(val));
  }
}
