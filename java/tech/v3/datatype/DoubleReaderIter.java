package tech.v3.datatype;

import it.unimi.dsi.fastutil.doubles.DoubleIterator;


public class DoubleReaderIter implements IOBase, DoubleIterator
{
  long idx;
  long num_elems;
  DoubleReader reader;
  public DoubleReaderIter(DoubleReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public double nextDouble() {
    double retval = reader.read(idx);
    ++idx;
    return retval;
  }
}
