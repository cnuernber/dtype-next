package tech.v3.datatype;

import java.util.function.Consumer;
import clojure.lang.Numbers;


public class ObjectConsumers
{
  public static class SumResult implements Consumers.Result
  {
    public final Object value;
    public final long nElems;
    public SumResult(Object _value, long _nElems)
    {
      value = _value;
      nElems = _nElems;
    }
    public Consumers.Result combine(Consumers.Result other) {
      return new SumResult(Numbers.add(value, other.value()), nElems + other.nElems());
    }
    public Object value() { return value; }
    public long nElems() { return nElems; }
  }
  public static class Sum implements Consumers.StagedConsumer, Consumer
  {
    public Object value;
    public long nElems;
    public Sum() {
      value = 0;
      nElems = 0;
    }
    public void accept(Object data) {
      value = Numbers.add(value, data);
      nElems++;
    }
    public Consumers.Result result() {
      return new SumResult(value, nElems);
    }
  }
  public static class UnaryOpSum implements Consumers.StagedConsumer, Consumer
  {
    public final UnaryOperator op;
    public Object value;
    public long nElems;
    public UnaryOpSum(UnaryOperator _op) {
      value = 0;
      nElems = 0;
      op = _op;
    }
    public void accept(Object data) {
      value = Numbers.add(value, op.unaryObject(data));
      nElems++;
    }
    public Consumers.Result result() {
      return new SumResult(value, nElems);
    }
  }

  public static class BinOpResult implements Consumers.Result
  {
    public final BinaryOperator op;
    public Object value;
    public long nElems;
    public BinOpResult(BinaryOperator _op, Object _value, long _nElems) {
      op = _op;
      value = _value;
      nElems = _nElems;
    }
    public Object value() { return value; }
    public long nElems() { return nElems; }
    public Consumers.Result combine(Consumers.Result other) {
      return new BinOpResult(op, Numbers.add(value, (long)other.value()),
			     nElems + other.nElems());
    }
  }
  public static class BinaryOp implements Consumers.StagedConsumer, Consumer
  {
    public final BinaryOperator op;
    public Object value;
    public long nElems;
    public BinaryOp(BinaryOperator _op, long initValue) {
      value = initValue;
      nElems = 0;
      op = _op;
    }
    public void accept(Object data) {
      value = op.binaryObject(value, data);
      nElems++;
    }
    public Consumers.Result result() {
      return new BinOpResult(op, value, nElems);
    }
  }
  //tight loop consume call.
  public static Consumers.Result consume(long offset, int grouplen, PrimitiveIO data,
					 Consumers.StagedConsumer consumer,
					 UnaryPredicate predicate) {
    @SuppressWarnings("unchecked")
    final Consumer<Object> dconsumer = (Consumer<Object>)consumer;
    if( predicate == null) {
      for (int idx = 0; idx < grouplen; ++idx) {
	dconsumer.accept(data.readObject((long)idx + offset));
      }
    } else {
      for (int idx = 0; idx < grouplen; ++idx) {
	Object dval = data.readObject((long)idx + offset);
	if(predicate.unaryObject(dval)) {
	  dconsumer.accept(dval);
	}
      }
    }
    return consumer.result();
  }
}
