package tech.v3.datatype;


import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;


public class DoubleConsumers
{
  public interface Result
  {
    public Result combine(Result rhs);
    public double value();
    public long nElems();
  }
  public interface StagedConsumer extends DoubleConsumer
  {
    public default DoubleConsumer andThen(DoubleConsumer next) {
      if (!(next instanceof StagedConsumer)) {
	throw new RuntimeException( "Argument is not a staged consumer");
      }
      return new ChainStagedConsumer(this, (StagedConsumer)next);
    }
    public Result result();
  }
  public static class MultiStagedConsumer implements StagedConsumer
  {
    public final StagedConsumer[] consumers;
    public MultiStagedConsumer(StagedConsumer[] _consumers) {
      this.consumers = _consumers;
    }
    public void accept(double val) {
      for (int idx = 0; idx < consumers.length; ++idx) {
	consumers[idx].accept(val);
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
    public double value() { throw new RuntimeException("Unimplemented"); }
    public long nElems() { throw new RuntimeException("Unimplemented"); }
  }



  public static class SumResult implements Result
  {
    public final double value;
    public final long nElems;
    public SumResult(double _value, long _nElems)
    {
      value = _value;
      nElems = _nElems;
    }
    public Result combine(Result other) {
      return new SumResult(value + other.value(), nElems + other.nElems());
    }
    public double value() { return value; }
    public long nElems() { return nElems; }
  }
  public static class Sum implements StagedConsumer
  {
    public double value;
    public long nElems;
    public Sum() {
      value = 0.0;
      nElems = 0;
    }
    public void accept(double data) {
      value += data;
      nElems++;
    }
    public Result result() {
      return new SumResult(value, nElems);
    }
  }
  public static class UnaryOpSum implements StagedConsumer
  {
    public final UnaryOperator op;
    public double value;
    public long nElems;
    public UnaryOpSum(UnaryOperator _op) {
      value = 0.0;
      nElems = 0;
      op = _op;
    }
    public void accept(double data) {
      value += op.unaryDouble(data);
      nElems++;
    }
    public Result result() {
      return new SumResult(value, nElems);
    }
  }

  public static class BinOpResult implements Result
  {
    public final BinaryOperator op;
    public double value;
    public long nElems;
    public BinOpResult(BinaryOperator _op, double _value, long _nElems) {
      op = _op;
      value = _value;
      nElems = _nElems;
    }
    public double value() { return value; }
    public long nElems() { return nElems; }
    public Result combine(Result other) {
      return new BinOpResult(op, value + other.value(), nElems + other.nElems());
    }
  }
  public static class BinaryOp implements StagedConsumer
  {
    public final BinaryOperator op;
    public double value;
    public long nElems;
    public BinaryOp(BinaryOperator _op, double initValue) {
      value = initValue;
      nElems = 0;
      op = _op;
    }
    public void accept(double data) {
      value = op.binaryDouble(value, data);
      nElems++;
    }
    public Result result() {
      return new BinOpResult(op, value, nElems);
    }
  }
  //tight loop consume call.
  public static Result consume(long offset, int grouplen, PrimitiveIO data,
			       StagedConsumer consumer, DoublePredicate predicate) {
    if( predicate == null) {
      for (int idx = 0; idx < grouplen; ++idx) {
	consumer.accept(data.readDouble((long)idx + offset));
      }
    } else {
      for (int idx = 0; idx < grouplen; ++idx) {
	double dval = data.readDouble((long)idx + offset);
	if(predicate.test(dval)) {
	  consumer.accept(dval);
	}
      }
    }
    return consumer.result();
  }
}
