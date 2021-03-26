package tech.v3.datatype;


import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import java.util.function.IntConsumer;
import java.util.function.DoublePredicate;
import java.util.List;
import clojure.lang.IDeref;


public class Consumers {
  //Consumers are created per in thread-independent contexts at each stage of
  //the reduction.
  public interface StagedConsumer extends IDeref
  {
    // Combine this with another.  This object is mutated
    public StagedConsumer combine(StagedConsumer other);
    // Post reduction get the values back.
    public default StagedConsumer combineList(List<StagedConsumer> others) {
      int n_consumers = others.size();
      StagedConsumer retval = this;
      for (int idx = 0; idx < n_consumers; ++idx ) {
	retval = retval.combine(others.get(idx));
      }
      return retval;
    }
    public default Object deref() { return value(); }
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
    public StagedConsumer combine(StagedConsumer _other) {
      MultiStagedConsumer other = (MultiStagedConsumer)_other;
      int n_consumers = consumers.length;
      StagedConsumer[] newConsumers = new StagedConsumer[consumers.length];
      for (int idx = 0; idx < consumers.length; ++idx) {
	newConsumers[idx] = consumers[idx].combine(other.consumers[idx]);
      }
      return new MultiStagedConsumer(newConsumers);
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
