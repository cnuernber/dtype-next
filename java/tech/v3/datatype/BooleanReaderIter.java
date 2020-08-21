package tech.v3.datatype;

import it.unimi.dsi.fastutil.booleans.BooleanIterator;


public class BooleanReaderIter implements IOBase, BooleanIterator
{
  long idx;
  long num_elems;
  BooleanReader reader;
  public BooleanReaderIter(BooleanReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public boolean nextBoolean() {
    boolean retval = reader.read(idx);
    ++idx;
    return retval;
  }
}
