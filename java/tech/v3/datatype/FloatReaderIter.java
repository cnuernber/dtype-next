package tech.v3.datatype;

import it.unimi.dsi.fastutil.floats.FloatIterator;


public class FloatReaderIter implements IOBase, FloatIterator
{
  long idx;
  long num_elems;
  FloatReader reader;
  public FloatReaderIter(FloatReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public float nextFloat() {
    float retval = reader.read(idx);
    ++idx;
    return retval;
  }
}
