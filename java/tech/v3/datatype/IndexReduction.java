package tech.v3.datatype;


import java.util.function.BiFunction;

public interface IndexReduction
{
  public Object reduceIndex(Object ctx, long idx);
  public Object reduceReductions(Object lhsCtx, Object rhsCtx);
  public default Object finalize(Object ctx, long nElems)
  {
    return ctx;
  }

  //This is used during concurrent map compute operations in group-by type reductions
  public class IndexedBiFunction implements BiFunction<Object,Object,Object>
  {
    public long index;
    public IndexReduction reducer;
    public IndexedBiFunction(IndexReduction rd)
    {
      index = 0;
      reducer = rd;
    }
    public void setIndex(long idx) {this.index = idx;}

    public Object apply(Object key, Object value) {
      return reducer.reduceIndex(value, index);
    }
  }
}
