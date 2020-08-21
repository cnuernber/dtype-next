package tech.v3.datatype;

import it.unimi.dsi.fastutil.shorts.ShortIterator;


public class ShortReaderIter implements IOBase, ShortIterator
{
  long idx;
  long num_elems;
  ShortReader reader;
  public ShortReaderIter(ShortReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public short nextShort() {
    short retval = reader.read(idx);
    ++idx;
    return retval;
  }
  public short current() {
    return reader.read(idx);
  }
}
