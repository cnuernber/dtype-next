package tech.v3.datatype;


import java.util.function.LongConsumer;


public class LongConsumers
{
  public static class SumResult implements Consumers.Result
  {
    public final long value;
    public final long nElems;
    public SumResult(long _value, long _nElems)
    {
      value = _value;
      nElems = _nElems;
    }
    public Consumers.Result combine(Consumers.Result other) {
      return new SumResult(value + (long)other.value(), nElems + other.nElems());
    }
    public Object value() { return value; }
    public long nElems() { return nElems; }
  }
  public static class Sum implements Consumers.StagedConsumer, LongConsumer
  {
    public long value;
    public long nElems;
    public Sum() {
      value = 0;
      nElems = 0;
    }
    public void accept(long data) {
      value += data;
      nElems++;
    }
    public Consumers.Result result() {
      return new SumResult(value, nElems);
    }
  }
  public static class UnaryOpSum implements Consumers.StagedConsumer, LongConsumer
  {
    public final UnaryOperator op;
    public long value;
    public long nElems;
    public UnaryOpSum(UnaryOperator _op) {
      value = 0;
      nElems = 0;
      op = _op;
    }
    public void accept(long data) {
      value += op.unaryLong(data);
      nElems++;
    }
    public Consumers.Result result() {
      return new SumResult(value, nElems);
    }
  }

  public static class BinOpResult implements Consumers.Result
  {
    public final BinaryOperator op;
    public long value;
    public long nElems;
    public BinOpResult(BinaryOperator _op, long _value, long _nElems) {
      op = _op;
      value = _value;
      nElems = _nElems;
    }
    public Object value() { return value; }
    public long nElems() { return nElems; }
    public Consumers.Result combine(Consumers.Result other) {
      return new BinOpResult(op, value + (long)other.value(),
			     nElems + other.nElems());
    }
  }
  public static class BinaryOp implements Consumers.StagedConsumer, LongConsumer
  {
    public final BinaryOperator op;
    public long value;
    public long nElems;
    public BinaryOp(BinaryOperator _op, long initValue) {
      value = initValue;
      nElems = 0;
      op = _op;
    }
    public void accept(long data) {
      value = op.binaryLong(value, data);
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
    final LongConsumer dconsumer = (LongConsumer)consumer;
    if( predicate == null) {
      for (int idx = 0; idx < grouplen; ++idx) {
	dconsumer.accept(data.readLong((long)idx + offset));
      }
    } else {
      for (int idx = 0; idx < grouplen; ++idx) {
	long dval = data.readLong((long)idx + offset);
	if(predicate.unaryLong(dval)) {
	  dconsumer.accept(dval);
	}
      }
    }
    return consumer.result();
  }
}
