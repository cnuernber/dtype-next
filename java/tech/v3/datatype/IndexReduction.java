package tech.v3.datatype;


import java.util.function.BiFunction;
import java.util.function.LongPredicate;
import java.util.List;
import java.util.Iterator;
import clojure.lang.IFn;

public interface IndexReduction
{
  //This allows us to handle sequences of readers as opposed to just a single reader
  //Or, when dealing with datasets, sequences of datasets.
  public default Object prepareBatch(Object batchData) { return batchData; }
  //Pre-filter the indexes before processing them.
  public default LongPredicate indexFilter(Object batchData) { return null; }
  public Object reduceIndex(Object batchData, Object ctx, long idx);
  public Object reduceReductions(Object lhsCtx, Object rhsCtx);
  public default Object reduceReductionList(Iterable contexts) {
    Iterator iter = contexts.iterator();
    Object init = iter.next();
    while(iter.hasNext()) {
      init = reduceReductions(init, iter.next());
    }
    return init;
  }
  public default Object finalize(Object ctx)
  {
    return ctx;
  }

  //This is used during concurrent map compute operations in group-by type reductions
  public class IndexedBiFunction implements BiFunction<Object,Object,Object>
  {
    public long index;
    public Object batchData;
    public final IndexReduction reducer;
    public IndexedBiFunction(IndexReduction rd, Object _batchData)
    {
      index = 0;
      batchData = _batchData;
      reducer = rd;
    }
    public void setIndex(long idx) {this.index = idx;}

    public Object apply(Object key, Object value) {
      return reducer.reduceIndex(batchData, value, index);
    }
  }
}
