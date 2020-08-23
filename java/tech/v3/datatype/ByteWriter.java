package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface ByteWriter extends PrimitiveWriter
{
  void write(long idx, byte value);
  default void writeBoolean(long idx, boolean val) {
    write(idx, (byte)(val ? 1 : 0));
  }
  default void writeByte(long idx, byte val) {
    write(idx, val);
  }
  default void writeShort(long idx, short val) {
    write(idx, RT.byteCast(val));
  }    
  default void writeChar(long idx, char val) {
    write(idx, RT.byteCast(val));
  }
  default void writeInt(long idx, int val) {
    write(idx, RT.byteCast(val));
  }
  default void writeLong(long idx, long val) {
    write(idx, RT.byteCast(val));
  }
  default void writeFloat(long idx, float val) {
    write(idx, RT.byteCast(val));
  }
  default void writeDouble(long idx, double val) {
    write(idx, RT.byteCast(val));
  }
  default void writeObject(long idx, Object val) {
    write(idx, RT.byteCast(val));
  }
  default Object elemwiseDatatype () { return Keyword.intern(null, "int8"); }
}
