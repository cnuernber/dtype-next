package tech.v3.datatype;

import java.util.Iterator;

public class ObjectReaderIter implements IOBase, Iterator
{
  long idx;
  long num_elems;
  ObjectReader reader;
  public ObjectReaderIter(ObjectReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public Object next() {
    Object retval = reader.read(idx);
    ++idx;
    return retval;
  }
}
