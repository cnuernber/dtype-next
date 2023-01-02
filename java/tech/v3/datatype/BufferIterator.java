package tech.v3.datatype;

import java.util.Iterator;

public interface BufferIterator extends ElemwiseDatatype, Iterator<Object>
{
  long nextLong();
  double nextDouble();
}
