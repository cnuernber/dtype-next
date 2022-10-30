package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import ham_fisted.Casts;



public interface DoubleBuffer extends Buffer
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "float64"); }
  default long readLong(long idx) {return Casts.longCast(readDouble(idx));}
  default Object readObject(long idx) {return readDouble(idx);}
  default void writeLong(long idx, long val) {
    writeDouble(idx, (double)val);
  }
  default void writeObject(long idx, Object val) {
    writeDouble(idx, RT.doubleCast(val));
  }
}
