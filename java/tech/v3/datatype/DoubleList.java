package tech.v3.datatype;


import clojure.lang.RT;


public interface DoubleList extends PrimitiveReader
{
  void ensureCapacity(long cap);
  void addDouble(double val);
  default boolean add(Object val) {
    addDouble(RT.doubleCast(val));
    return true;
  }
}
