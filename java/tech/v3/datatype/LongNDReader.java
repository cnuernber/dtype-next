package tech.v3.datatype;


public interface LongNDReader extends LongReader
{
  Iterable shape();
  int rank();
  long outermostDim();
  long ndReadLong(long idx);
  long ndReadLong(long row, long col);
  long ndReadLong(long height, long width, long chan);
  long ndReadLong(long w, long height, long width, long chan);
  long ndReadLongIter(Iterable idxs);
}
