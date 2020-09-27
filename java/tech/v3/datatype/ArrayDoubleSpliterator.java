package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.Spliterator;
import java.util.Objects;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import org.apache.commons.math3.exception.NotANumberException;


//Spliterators are a truly odd form of parallelism but they are what
//java decided on.  They have a strong advantage in things like
//hashtables where they can split and still iterate over an undefined
//number of things.  They are stricly slower, however, than a parallelized
//index reduction but they are quite a bit more composeable.
public class PrimitiveIODoubleSpliterator implements Spliterator.OfDouble
{
  public static final Keyword keep = Keyword.intern(null, "keep");
  public static final Keyword remove = Keyword.intern(null, "remove");
  public static final DoublePredicate removePredicate = (d)-> !Double.isNaN(d);
  public static final Keyword exception = Keyword.intern(null, "exception");
  public static final DoublePredicate exceptPredicate = new DoublePredicate() {
      public boolean test(double d) {
	if (Double.isNaN(d) ) {
	  throw new NotANumberException();
	}
	return true;
      }
    };

  public final PrimitiveIO reader;
  long index;
  long nElems;
  public final DoublePredicate predicate;

  public PrimitiveIODoubleSpliterator( PrimitiveIO _reader, long _index, long _nElems,
				       Object nanStrategyOrPredicate )
  {
    Objects.requireNonNull(_reader);
    reader = _reader;
    index = _index;
    nElems = _nElems;
    if ( nanStrategyOrPredicate instanceof DoublePredicate ) {
      predicate = (DoublePredicate) nanStrategyOrPredicate;
    } else if ( nanStrategyOrPredicate instanceof Keyword ) {
      if ( keep == nanStrategyOrPredicate ) {
	predicate = null;
      } else if ( remove == nanStrategyOrPredicate ) {
	predicate = removePredicate;
      } else if ( exception == nanStrategyOrPredicate ) {
	predicate = exceptPredicate;
      }
      else {
	throw new RuntimeException(String.format("Unrecognized keyword: %s", nanStrategyOrPredicate));
      }
    } else if (nanStrategyOrPredicate != null) {
      throw new RuntimeException(String.format("Unrecognized nan strat or predicate: %s", nanStrategyOrPredicate));
    } else {
      //Null predicate is OK
      predicate = null;
    }
  }
  public Spliterator.OfDouble trySplit() {
    long nLeft = nElems - index;
    //It appears the stream algorithms have assumptions about left vs. right iteration
    //trySplit should return the left iterator which means it iterates over a lower range
    //than existing iterator
    if (nLeft > 2) {
      long midpoint = index + (nLeft / 2);
      long prevIndex = index;
      index = midpoint;
      //System.out.println(String.format("trySplit (nonnull): (%d,%d)->(%d,%d),(%d,%d)", prevIndex, nElems,
      //                                  prevIndex, midpoint, midpoint, nElems));
      return new PrimitiveIODoubleSpliterator(reader, prevIndex, midpoint, predicate);
    }
    // System.out.println(String.format("trySplit (null): index %d, nElems %d", index, nElems));
    return null;
  }
  public boolean tryAdvance(DoubleConsumer action) {
    //System.out.println(String.format("tryAdvance: index %d, nElems %d", index, nElems));
    if ( predicate == null) {
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
	if( predicate.test(nextValue) ) {
	  action.accept(nextValue);
	  return true;
	} else {
	  hasMore = index < nElems;
	}
      }
    }
    return false;
  }
  //Override to limit nanStrategy checks
  public void forEachRemaining(DoubleConsumer action) {
    //System.out.println(String.format("forEachRemaining (nonnull): index %d, nElems %d", index, nElems));
    if ( predicate == null ) {
      while( index < nElems ) {
	action.accept(reader.readDouble(index));
	++index;
      }
    } else {
      while( index < nElems ) {
	double dvalue = reader.readDouble(index);
	++index;
	if( predicate.test(dvalue) ) {
	  action.accept( dvalue );
	}
      }
    }
  }
  public int characteristics() {
    return Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.SIZED |
      Spliterator.SUBSIZED | Spliterator.ORDERED;
  }
  public long estimateSize() {
    return nElems - index;
  }
}
