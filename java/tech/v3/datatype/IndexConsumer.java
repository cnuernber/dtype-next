package tech.v3.datatype;


import java.util.function.LongConsumer;
import java.util.List;
import ham_fisted.Reducible;
import ham_fisted.MutArrayMap;
import ham_fisted.BitmapTrieCommon;
import ham_fisted.IMutList;
import clojure.lang.IDeref;
import clojure.lang.IFn;
import clojure.lang.IObj;
import clojure.lang.Keyword;


public class IndexConsumer
  implements LongConsumer, Reducible, IDeref {
  final IFn rangeFn;
  final IMutList list;
  long firstVal;
  long lastVal;
  long increment;
  long minVal;
  long maxVal;

  public IndexConsumer(IFn rangeFn, IMutList list) {
    this.rangeFn = rangeFn;
    this.list = list;
    firstVal = Long.MIN_VALUE;
    lastVal = Long.MIN_VALUE;
    increment = Long.MIN_VALUE;
    minVal = Long.MAX_VALUE;
    maxVal = Long.MIN_VALUE;
  }

  public IndexConsumer(IFn rangeFn, IMutList list, long fv, long lv,
		       long incr, long minv, long maxv) {
    this.rangeFn = rangeFn;
    this.list = list;
    this.firstVal = fv;
    this.lastVal = lv;
    this.increment = incr;
    this.minVal = minv;
    this.maxVal = maxv;
  }
  public String toString() {
    if(list.isEmpty())
      return MutArrayMap.createKV(BitmapTrieCommon.defaultHashProvider,
				  Keyword.intern("firstVal"), firstVal,
				  Keyword.intern("lastVal"), lastVal,
				  Keyword.intern("increment"), increment,
				  Keyword.intern("min"), minVal,
				  Keyword.intern("max"), maxVal).toString();
    else
      return list.toString();
  }
  @SuppressWarnings("unchecked")
  void addToList(IMutList list) {
    if(increment == Long.MIN_VALUE) {
      if(firstVal != Long.MIN_VALUE)
	list.addLong(firstVal);
    } else if(increment == Long.MAX_VALUE) {
      list.addAll(this.list);
    } else {
      list.addAll((List)rangeFn.invoke(firstVal, lastVal + increment, increment));
    }
  }

  public void accept(long lval) {
    if(firstVal == Long.MIN_VALUE) {
      firstVal = lval;
    } else {
      final long newInc = lval - lastVal;
      final long curInc = increment;
      //Put both common cases first.
      if(curInc == Long.MAX_VALUE) {
	list.addLong(lval);
      } else if (curInc == newInc) {
	//intentionally left empty.
      } else if (newInc != 0 && curInc == Long.MIN_VALUE) {
	increment = newInc;
      } else {
	addToList(list);
	increment = Long.MAX_VALUE;
	list.addLong(lval);
      }
    }
    lastVal = lval;
    minVal = Math.min(lval, minVal);
    maxVal = Math.max(lval, maxVal);
  }

  public IndexConsumer reduce(Reducible _other) {
    final IndexConsumer other = (IndexConsumer)_other;
    if(firstVal == Long.MIN_VALUE) return other;
    if(other.firstVal == Long.MIN_VALUE) return this;
    final long newMin = Math.min(this.minVal, other.minVal);
    final long newMax = Math.max(this.maxVal, other.maxVal);
    if(increment != Long.MAX_VALUE &&
       increment == other.increment &&
       (lastVal + increment) == other.firstVal) {
      return new IndexConsumer(rangeFn, list, firstVal, other.lastVal, increment, newMin, newMax);
    } else {
      if(increment != Long.MAX_VALUE)
	addToList(list);
      other.addToList(list);
      return new IndexConsumer(rangeFn, list, firstVal, (Long)list.invoke(-1), Long.MAX_VALUE,
			       newMin, newMax);
    }
  }

  public Object deref() {
    IObj retval;
    if(firstVal == Long.MIN_VALUE)
      retval = (IObj)rangeFn.invoke(0);
    else if (increment == Long.MAX_VALUE)
      retval = (IObj)list;
    else
      retval = (IObj)rangeFn.invoke(firstVal, lastVal + increment, increment);
    return retval.withMeta(MutArrayMap.createKV(BitmapTrieCommon.defaultHashProvider,
						Keyword.intern("min"), minVal,
						Keyword.intern("max"), maxVal).persistent());
  }
}
