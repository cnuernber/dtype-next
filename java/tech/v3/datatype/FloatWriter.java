package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface FloatWriter extends PrimitiveWriter
{
  void write(long idx, float value);
  default void writeBoolean(long idx, boolean val) {
    write(idx, (val ? 1.0f : 0.0f));
  }
  default void writeByte(long idx, byte val) {
    write(idx, (float)val);
  }
  default void writeShort(long idx, short val) {
    write(idx, (float)val);
  }
  default void writeChar(long idx, char val) {
    write(idx, (float)val);
  }
  default void writeInt(long idx, int val) {
    write(idx, RT.floatCast(val));
  }
  default void writeLong(long idx, long val) {
    write(idx, RT.floatCast(val));
  }
  default void writeFloat(long idx, float val) {
    write(idx, val);
  }
  default void writeDouble(long idx, double val) {
    write(idx, RT.floatCast(val));
  }
  default void writeObject(long idx, Object val) {
    write(idx, RT.floatCast(val));
  }
  default Object elemwiseDatatype () { return Keyword.intern(null, "float32"); }
};
