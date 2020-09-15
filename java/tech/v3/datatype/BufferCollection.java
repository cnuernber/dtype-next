package tech.v3.datatype;


import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;


public interface BufferCollection extends Collection
{
  //Methods missing
  //Iterator iterator();
  //int size();
  //Object[] toArray();
  //<T> T[] toArray(T[] prototype);
  
  default boolean add(Object e) {throw new UnsupportedOperationException("Unimplemented"); }
  default boolean addAll(Collection c) {throw new UnsupportedOperationException("Unimplemented"); }
  default void clear() { throw new UnsupportedOperationException("Unimplemented"); }
  default boolean contains(Object e) {throw new UnsupportedOperationException("Unimplemented");}
  default boolean isEmpty() { return 0 == size(); }
  default boolean remove(Object o) {throw new UnsupportedOperationException("Unimplemented");}
  default boolean removeAll(Collection c) {throw new UnsupportedOperationException("Unimplemented");}
  default boolean retainAll(Collection c) {throw new UnsupportedOperationException("Unimplemented");}
  default Object[] toArray() {
    int nElems = size();
    Object[] retval = new Object[size()];
    Iterator iter = iterator();
    int idx = 0;
    while (iter.hasNext()) {
      retval[idx++] = iter.next();
    }
    return retval;
  }  
}
