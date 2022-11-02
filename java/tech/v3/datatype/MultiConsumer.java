package tech.v3.datatype;


import java.util.function.Consumer;
import java.util.function.DoubleConsumer;
import java.util.function.LongConsumer;
import ham_fisted.Reducible;
import clojure.lang.IDeref;

public class MultiConsumer implements Consumer, DoubleConsumer,
				      LongConsumer, Reducible,
				      IDeref
{
  public final Reducible[] consumers;
  public MultiConsumer(Reducible[] c) { this.consumers = c; }
  public void accept(Object val) {
    for (int idx = 0; idx < consumers.length; ++idx) {
      ((Consumer)consumers[idx]).accept(val);
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
  public Reducible reduce(Reducible o) {
    final MultiConsumer other = (MultiConsumer)o;
    final int nc = consumers.length;
    final Reducible[] newc = new Reducible[nc];
    for (int idx = 0; idx < nc; ++idx) {
      newc[idx] = consumers[idx].reduce(other.consumers[idx]);
    }
    return new MultiConsumer(newc);
  }
  public Object deref() {
    Object[] values = new Object[consumers.length];
    for (int idx = 0; idx < consumers.length; ++idx) {
      values[idx] = ((IDeref)consumers[idx]).deref();
    }
    return values;
  }
}
