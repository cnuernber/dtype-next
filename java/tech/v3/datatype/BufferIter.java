package tech.v3.datatype;

import java.util.Iterator;

public class BufferIter implements BufferIterator, ECount
{
  long idx;
  long num_elems;
  Buffer reader;
  public BufferIter(Buffer _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public long nextLong() {
    long retval = reader.readLong(idx);
    ++idx;
    return retval;
  }
  public double nextDouble() {
    double retval = reader.readDouble(idx);
    ++idx;
    return retval;
  }
  public Object next() {
    Object retval = reader.readObject(idx);
    ++idx;
    return retval;
  }
}
