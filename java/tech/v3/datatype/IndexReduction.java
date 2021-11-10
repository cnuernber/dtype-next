package tech.v3.datatype;


import java.util.function.BiFunction;
import java.util.List;

public interface IndexReduction
{
  //This allows us to handle sequences of readers as opposed to just a single reader
  //Or, when dealing with datasets, sequences of datasets.
  public default Object prepareBatch(Object batchData) { return batchData; }
  public default boolean filterIndex(Object batchData, long idx) { return true; }
  public Object reduceIndex(Object batchData, Object ctx, long idx);
  public Object reduceReductions(Object lhsCtx, Object rhsCtx);
  public default Object reduceReductionList(List contexts) {
    int n_contexts = contexts.size();
    if ( 1 == n_contexts ) {
      return contexts.get(0);
    } else {
      Object retval = contexts.get(0);
      for (int idx = 1; idx < n_contexts; ++idx ) {
	retval = reduceReductions(retval, contexts.get(idx));
      }
      return retval;
    }
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
