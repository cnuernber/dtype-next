package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.RT;
import org.roaringbitmap.IntIterator;


public class LongBitmapIter implements BufferIterator
{
  IntIterator iter;
  public LongBitmapIter(IntIterator _iter)
  {
    iter = _iter;
  }
  public Object elemwiseDatatype() { return Keyword.intern(null, "uint32"); }
  public boolean hasNext() { return iter.hasNext(); }
  public long nextLong() {
    return Integer.toUnsignedLong( iter.next() );
  }

  public boolean nextBoolean() { return BooleanConversions.from(nextLong()); }
  public byte nextByte() { return RT.byteCast(nextLong()); }
  public short nextShort() { return RT.shortCast(nextLong()); }
  public char nextChar() { return (char)nextLong(); }
  public int nextInt() { return RT.intCast(nextLong()); }
  public float nextFloat() { return RT.floatCast(nextLong()); }
  public double nextDouble() { return (double)nextLong(); }
  public Object next() { return nextLong(); }
}
