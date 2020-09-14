package tech.v3.datatype;

import java.util.Iterator;

public class PrimitiveIOIter implements PrimitiveIOIterator, Countable
{
  long idx;
  long num_elems;
  PrimitiveIO reader;
  public PrimitiveIOIter(PrimitiveIO _reader)
  {
    idx = 0;
    num_elems = _reader.lsize();
    reader = _reader;
  }
  public Object elemwiseDatatype() { return reader.elemwiseDatatype(); }
  public long lsize() { return num_elems - idx; }
  public boolean hasNext() { return idx < num_elems; }
  public boolean nextBoolean() {
    boolean retval = reader.readBoolean(idx);
    ++idx;
    return retval;
  }
  public byte nextByte() {
    byte retval = reader.readByte(idx);
    ++idx;
    return retval;
  }
  public short nextShort() {
    short retval = reader.readShort(idx);
    ++idx;
    return retval;
  }
  public char nextChar() {
    char retval = reader.readChar(idx);
    ++idx;
    return retval;
  }
  public int nextInt() {
    int retval = reader.readInt(idx);
    ++idx;
    return retval;
  }
  public long nextLong() {
    long retval = reader.readLong(idx);
    ++idx;
    return retval;
  }
  public float nextFloat() {
    float retval = reader.readFloat(idx);
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
