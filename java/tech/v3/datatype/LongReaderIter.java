package tech.v3.datatype;

import it.unimi.dsi.fastutil.longs.LongIterator;


public class LongReaderIter implements IOBase, LongIterator
{
  long idx;
  long num_elems;
  LongReader reader;
  public LongReaderIter(LongReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public long nextLong() {
    long retval = reader.read(idx);
    ++idx;
    return retval;
  }
}
