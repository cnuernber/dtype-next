package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface ShortWriter extends PrimitiveWriter
{
  void write(long idx, short value);
  default void writeBoolean(long idx, boolean val) {
    write(idx, (byte)(val ? 1 : 0));
  }
  default void writeByte(long idx, byte val) {
    write(idx, (short)val);
  }
  default void writeShort(long idx, short val) {
    write(idx, val);
  }
  default void writeChar(long idx, char val) {
    write(idx, RT.shortCast(val));
  }
  default void writeInt(long idx, int val) {
    write(idx, RT.shortCast(val));
  }
  default void writeLong(long idx, long val) {
    write(idx, RT.shortCast(val));
  }
  default void writeFloat(long idx, float val) {
    write(idx, RT.shortCast(val));
  }
  default void writeDouble(long idx, double val) {
    write(idx, RT.shortCast(val));
  }
  default void writeObject(long idx, Object val) {
    write(idx, RT.shortCast(val));
  }
  default Object elemwiseDatatype () { return Keyword.intern(null, "int16"); }

}
