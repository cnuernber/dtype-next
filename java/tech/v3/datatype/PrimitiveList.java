package tech.v3.datatype;



public interface PrimitiveList extends PrimitiveIO
{
  void ensureCapacity(long cap);
  void addBoolean(boolean val);
  void addDouble(double val);
  void addLong(long val);
  void addObject(Object val);
  default boolean add(Object val) {
    addObject(val);
    return true;
  }
}
