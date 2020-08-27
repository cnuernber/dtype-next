package tech.v3.datatype;


import clojure.lang.RT;


public interface BooleanList extends PrimitiveReader
{
  void ensureCapacity(long cap);
  void addBoolean(boolean val);
  default boolean add(Object val) {
    addBoolean(RT.booleanCast(val));
    return true;
  }
}
