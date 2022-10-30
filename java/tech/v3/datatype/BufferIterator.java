package tech.v3.datatype;

import java.util.Iterator;

public interface BufferIterator extends ElemwiseDatatype, Iterator
{
  long nextLong();
  double nextDouble();
}
