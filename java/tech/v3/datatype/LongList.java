package tech.v3.datatype;


import clojure.lang.RT;


public interface LongList extends PrimitiveReader
{
  void ensureCapacity(long cap);
  void addLong(long val);
  default boolean add(Object val) {
    addLong(RT.longCast(val));
    return true;
  }
}
