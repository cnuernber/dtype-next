package tech.v3.datatype;


import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;
import clojure.lang.ISeq;
import java.util.List;
import java.util.RandomAccess;


public interface PrimitiveWriter extends IOBase, IFn,
					 List, RandomAccess
{
  void writeBoolean(long idx, boolean val);
  void writeByte(long idx, byte val);
  void writeShort(long idx, short val);
  void writeChar(long idx, char val);
  void writeInt(long idx, int val);
  void writeLong(long idx, long val);
  void writeFloat(long idx, float val);
  void writeDouble(long idx, double val);
  void writeObject(long idx, Object val);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object set(int idx, Object val) {
    writeObject(idx, val);
    return null;
  }
  default boolean isEmpty() { return lsize() == 0; }
  default Object invoke(Object arg, Object arg2) {
    writeObject(RT.uncheckedLongCast(arg), arg2);
    return null;
  }
  default Object applyTo(ISeq items) {
    if (2 != items.count()) {
      throw new RuntimeException("Must have 2 arguments"); 
    } else {
      //Abstract method error
      return invoke(items.first(), items.next());
    }
  }
}
