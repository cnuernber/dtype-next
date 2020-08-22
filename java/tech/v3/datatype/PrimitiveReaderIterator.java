package tech.v3.datatype;

import java.util.Iterator;

public interface PrimitiveReaderIterator extends IOBase, Iterator
{
  boolean nextBoolean();
  byte nextByte();
  short nextShort();
  char nextChar();
  int nextInt();
  long nextLong();
  float nextFloat();
  double nextDouble();
}
