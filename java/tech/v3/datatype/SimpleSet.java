package tech.v3.datatype;


import java.util.AbstractSet;
import java.util.Iterator;
import java.util.function.Predicate;



public class SimpleSet extends AbstractSet {
  public final Iterable vals;
  public final Predicate containsFn;
  public final int nElems;

  public SimpleSet(int _nElems, Iterable _vals, Predicate _containsFn) {
    vals = _vals;
    containsFn = _containsFn;
    nElems = _nElems;
  }
  public int size() { return nElems; }
  public Iterator iterator() { return vals.iterator(); }
  public boolean contains(Object o) { return containsFn.test(o); }
}
