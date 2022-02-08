package tech.v3.datatype;


import java.util.AbstractCollection;
import java.util.Iterator;


public class SimpleCollection extends AbstractCollection {
  public final Iterable iterable;
  public final int size;

  public SimpleCollection(Iterable _it, int _size) {
    iterable = _it;
    size = _size;
  }
  public int size() { return size; }
  public Iterator iterator() { return iterable.iterator(); }
}
