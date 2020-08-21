package tech.v3.datatype;

import it.unimi.dsi.fastutil.ints.IntIterator;


public class IntReaderIter implements IOBase, IntIterator
{
  long idx;
  long num_elems;
  IntReader reader;
  public IntReaderIter(IntReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public int nextInt() {
    int retval = reader.read(idx);
    ++idx;
    return retval;
  }
}
