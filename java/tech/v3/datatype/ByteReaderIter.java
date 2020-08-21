package tech.v3.datatype;

import it.unimi.dsi.fastutil.bytes.ByteIterator;


public class ByteReaderIter implements IOBase, ByteIterator
{
  long idx;
  long num_elems;
  ByteReader reader;
  public ByteReaderIter(ByteReader _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public byte nextByte() {
    byte retval = reader.read(idx);
    ++idx;
    return retval;
  }
}
