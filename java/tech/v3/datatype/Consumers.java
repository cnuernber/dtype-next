package tech.v3.datatype;


import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import java.util.function.IntConsumer;
import java.util.function.DoublePredicate;


public class Consumers {
  //Consumers are created per in thread-independent contexts at each stage of
  //the reduction.
  public interface StagedConsumer
  {
    // Combine this with another.  This object is mutated
    public void inplaceCombine(StagedConsumer other);
    // Post reduction get the values back.
    public Object value();
  }
  public static class MultiStagedConsumer implements StagedConsumer,
						     IntConsumer,
						     LongConsumer,
						     DoubleConsumer
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
    public void inplaceCombine(StagedConsumer _other) {
      MultiStagedConsumer other = (MultiStagedConsumer)_other;
      for (int idx = 0; idx < consumers.length; ++idx) {
	consumers[idx].inplaceCombine(other.consumers[idx]);
      }
    }
    public Object value() {
      Object[] values = new Object[consumers.length];
      for (int idx = 0; idx < consumers.length; ++idx) {
	values[idx] = consumers[idx].value();
      }
      return values;
    }
  }
}
