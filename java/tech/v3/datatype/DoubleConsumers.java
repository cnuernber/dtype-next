package tech.v3.datatype;


import java.util.function.DoubleConsumer;


public class DoubleConsumers
{
  public static class DoubleConsumerResult
  {
    public final double value;
    public final long nElems;
    public DoubleConsumerResult(double _value, long _nElems)
    {
      value = _value;
      nElems = _nElems;
    }
    public DoubleConsumerResult combine(DoubleConsumerResult other) {
      return new DoubleConsumerResult(value + other.value, nElems + other.nElems);
    }
  }
  public interface ConsumerResult
  {
    public DoubleConsumerResult result();
  }
  public static class SummationConsumer implements DoubleConsumer, ConsumerResult
  {
    public double value;
    public long nElems;
    public SummationConsumer() {
      value = 0.0;
      nElems = 0;
    }
    public void accept(double data) {
      value += data;
      nElems++;
    }
    public DoubleConsumerResult result() {
      return new DoubleConsumerResult(value, nElems);
    }
  }
  public static class UnaryOpSummationConsumer implements DoubleConsumer, ConsumerResult
  {
    public final UnaryOperator op;
    public double value;
    public long nElems;
    public UnaryOpSummationConsumer(UnaryOperator _op) {
      value = 0.0;
      nElems = 0;
      op = _op;
    }
    public void accept(double data) {
      value += op.unaryDouble(data);
      nElems++;
    }
    public DoubleConsumerResult result() {
      return new DoubleConsumerResult(value, nElems);
    }
  }
  public static class BinaryOpConsumer implements DoubleConsumer, ConsumerResult
  {
    public final BinaryOperator op;
    public double value;
    public long nElems;
    public BinaryOpConsumer(BinaryOperator _op, double initValue) {
      value = initValue;
      nElems = 0;
      op = _op;
    }
    public void accept(double data) {
      value = op.binaryDouble(value, data);
      nElems++;
    }
    public DoubleConsumerResult result() {
      return new DoubleConsumerResult(value, nElems);
    }
  }
}
