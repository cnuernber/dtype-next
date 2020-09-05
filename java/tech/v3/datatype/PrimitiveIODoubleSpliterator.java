package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.Spliterator;
import java.util.function.DoubleConsumer;
import org.apache.commons.math3.exception.NotANumberException;


//Spliterators are a truly odd form of parallelism but they are what
//java decided on.  They have a strong advantage in things like
//hashtables where they can split and still iterate over an undefined
//number of things.
public class PrimitiveIODoubleSpliterator implements Spliterator.OfDouble
{
  public static final Keyword keep = Keyword.intern(null, "keep");
  public static final Keyword remove = Keyword.intern(null, "remove");
  public static final Keyword exception = Keyword.intern(null, "exception");

  public final PrimitiveIO reader;
  long index;
  long nElems;
  public final Keyword nanStrategy;

  public PrimitiveIODoubleSpliterator( PrimitiveIO _reader, long _index, long _nElems,
				       Keyword _nanStrategy )
  {
    reader = _reader;
    index = _index;
    nElems = _nElems;
    nanStrategy = _nanStrategy;
  }
  public Spliterator.OfDouble trySplit() {
    long nLeft = nElems - index;
    if (nLeft > 1) {
      long oldNElems = nElems;
      nLeft /= 2;
      nElems = index + nLeft;
      return new PrimitiveIODoubleSpliterator(reader, nLeft, oldNElems, nanStrategy);
    }
    return null;
  }
  public boolean tryAdvance(DoubleConsumer action) {
    if ( keep == nanStrategy ) {
      if ( index < nElems ) {
	action.accept(reader.readDouble(index));
	++index;
	return true;
      }
    } else {
      boolean hasMore = index < nElems;
      while( hasMore ) {
	double nextValue = reader.readDouble(index);
	++index;
	if( Double.isNaN(nextValue) ) {
	  if ( exception == nanStrategy ) throw new NotANumberException();
	  hasMore = index < nElems;
	} else {
	  action.accept(nextValue);
	  return true;
	}
      }
    }
    return false;
  }
  //Override to limit nanStrategy checks
  public void forEachRemaining(DoubleConsumer action) {
    if ( keep == nanStrategy ) {
      while( index < nElems ) {
	action.accept(reader.readDouble(index));
	++index;
      }
    } else if ( remove == nanStrategy ) {
      while( index < nElems ) {
	double dvalue = reader.readDouble(index);
	++index;
	if( !Double.isNaN(dvalue) ) {
	  action.accept( dvalue );
	}
      }
    } else if ( exception == nanStrategy ) {
      while( index < nElems ) {
	double dvalue = reader.readDouble(index);
	++index;
	if(Double.isNaN(dvalue)) throw new NotANumberException();
	action.accept(dvalue);
      }
    }
  }
  public int characteristics() {
    return Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.SIZED |
      Spliterator.SUBSIZED;
  }
  public long estimateSize() {
    return nElems - index;
  }
}
