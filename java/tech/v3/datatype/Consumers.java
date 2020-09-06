package tech.v3.datatype;


import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import java.util.function.IntConsumer;
import java.util.function.DoublePredicate;


public class Consumers {
  public interface Result
  {
    public Result combine(Result rhs);
    public Object value();
    public long nElems();
  }
  //Consumers are created per in thread-independent contexts at each stage of
  //reduction.
  public interface StagedConsumer
  {
    public Result result();
  }
  public static class MultiStagedConsumer implements StagedConsumer, IntConsumer,
						     LongConsumer, DoubleConsumer
  {
    public final StagedConsumer[] consumers;
    public MultiStagedConsumer(StagedConsumer[] _consumers) {
      this.consumers = _consumers;
    }
    public void accept(int val) {
      for (int idx = 0; idx < consumers.length; ++idx) {
	((IntConsumer)consumers[idx]).accept(val);
      }
    }
    public void accept(long val) {
      for (int idx = 0; idx < consumers.length; ++idx) {
	((LongConsumer)consumers[idx]).accept(val);
      }
    }
    public void accept(double val) {
      for (int idx = 0; idx < consumers.length; ++idx) {
	((DoubleConsumer)consumers[idx]).accept(val);
      }
    }
    public Result result() {
      Result[] results = new Result[consumers.length];
      for (int idx = 0; idx < consumers.length; ++idx) {
	results[idx] = consumers[idx].result();
      }
      return new MultiStagedResult(results);
    }
  }
  public static class MultiStagedResult implements Result
  {
    public final Result[] results;
    public MultiStagedResult(Result[] _results) {
      this.results = _results;
    }
    public Result combine(Result _other) {
      if (! (_other instanceof MultiStagedResult) ) {
	throw new RuntimeException( "Result is not a multi result");
      }
      MultiStagedResult other = (MultiStagedResult)_other;
      Result[] newResults = new Result[results.length];
      for (int idx = 0; idx < results.length; ++idx) {
	newResults[idx] = results[idx].combine(other.results[idx]);
      }
      return new MultiStagedResult(newResults);
    }
    public Object value() {
      Object[] values = new Object[results.length];
      for (int idx = 0; idx < results.length; ++idx) {
	values[idx] = results[idx].value();
      }
      return values;
    }
    public long nElems() { return results[0].nElems(); }
  }
}
