package tech.v3.datatype;


import java.util.Collection;
import java.util.function.Consumer;


public interface PrimitiveList extends Buffer
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
  default boolean addAll(Collection coll) {
    if(coll != null) {
      coll.forEach(new Consumer() {
  	  public void accept(Object arg) {
  	    addObject(arg);
      }});
    }
    return true;
  }
}
