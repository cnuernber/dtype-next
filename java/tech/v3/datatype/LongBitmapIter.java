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

  public double nextDouble() { return (double)nextLong(); }
  public Object next() { return nextLong(); }
}
