package tech.v3.datatype;


import clojure.lang.RT;
import java.util.Collection;
import java.util.Iterator;


public interface BooleanList extends PrimitiveReader
{
  void addBoolean(boolean val);
  default boolean add(Object val) {
    addBoolean(RT.booleanCast(val));
    return true;
  }
}
