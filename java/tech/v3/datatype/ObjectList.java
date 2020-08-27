package tech.v3.datatype;


public interface ObjectList extends PrimitiveReader
{
  void ensureCapacity(long cap);
  void addObject(Object val);
  default boolean add(Object val) {
    addObject(val);
    return true;
  }
}
