package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface BooleanWriter extends PrimitiveWriter
{
  void write(long idx, boolean value);
  default void writeBoolean(long idx, boolean val) {
    write(idx, val);
  }
  default void writeByte(long idx, byte val) {
    write(idx, val != 0 ? true : false);
  }
  default void writeShort(long idx, short val) {
    write(idx, val != 0 ? true : false);
  }
  default void writeChar(long idx, char val) {
    write(idx, val != 0 ? true : false);
  }
  default void writeInt(long idx, int val) {
    write(idx, val != 0 ? true : false);
  }
  default void writeLong(long idx, long val) {
    write(idx, val != 0 ? true : false);
  }
  default void writeFloat(long idx, float val) {
    write(idx, val != 0.0f ? true : false);
  }
  default void writeDouble(long idx, double val) {
    write(idx, val != 0.0 ? true : false);
  }
  default void writeObject(long idx, Object val) {
    if (val instanceof Number)
      write(idx, RT.doubleCast(val) != 0.0 ? true : false);
    else if (val instanceof Boolean)
      write(idx, (boolean)val);
    else
      write(idx, val != null);
  }
  default Object elemwiseDatatype () { return Keyword.intern(null, "boolean"); }

}
