package tech.v3.datatype;


import clojure.lang.Keyword;
import ham_fisted.Casts;


public interface LongBuffer extends Buffer
{
  default Object elemwiseDatatype () { return Keyword.intern(null, "int64"); }
  default double readDouble(long idx) {return (double)readLong(idx);}
  default Object readObject(long idx) {return readLong(idx);}
  default void writeDouble(long idx, double val) {
    writeLong(idx, Casts.longCast(val));
  }
  default void writeObject(long idx, Object val) {
    writeLong(idx, Casts.longCast(val));
  }
}
