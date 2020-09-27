package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import java.util.HashMap;
import java.util.Collections;


public class DoubleConsumers
{
  public static class SumResult implements Consumers.Result
  {
    public final double value;
    public final long nElems;
    public SumResult(double _value, long _nElems)
    {
      value = _value;
      nElems = _nElems;
    }
    public Consumers.Result combine(Consumers.Result other) {
      return new SumResult(value + (double)other.value(), nElems + other.nElems());
    }
    public Object value() { return value; }
    public long nElems() { return nElems; }
  }
  public static class Sum implements Consumers.StagedConsumer, DoubleConsumer
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
    public Consumers.Result result() {
      return new SumResult(value, nElems);
    }
  }
  public static class UnaryOpSum implements Consumers.StagedConsumer, DoubleConsumer
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
    public Consumers.Result result() {
      return new SumResult(value, nElems);
    }
  }

  public static class BinOpResult implements Consumers.Result
  {
    public final BinaryOperator op;
    public double value;
    public long nElems;
    public BinOpResult(BinaryOperator _op, double _value, long _nElems) {
      op = _op;
      value = _value;
      nElems = _nElems;
    }
    public Object value() { return value; }
    public long nElems() { return nElems; }
    public Consumers.Result combine(Consumers.Result other) {
      return new BinOpResult(op,
			     op.binaryDouble(value, (double)other.value()),
			     nElems + other.nElems());
    }
  }
  public static class BinaryOp implements Consumers.StagedConsumer, DoubleConsumer
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
    public Consumers.Result result() {
      return new BinOpResult(op, value, nElems);
    }
  }
  public static class MinMaxSum implements Consumers.StagedConsumer, DoubleConsumer
  {
    public double sum;
    public double min;
    public double max;
    public long nElems;
    public MinMaxSum() {
      sum = 0.0;
      max = -Double.MAX_VALUE;
      min = Double.MAX_VALUE;
      nElems = 0;
    }
    public void accept(double val) {
      sum += val;
      min = Math.min(val, min);
      max = Math.max(val, max);
      nElems++;
    }
    public Consumers.Result result() {
      return new MinMaxSumResult(sum,min,max,nElems);
    }
  }
  public static class MinMaxSumResult implements Consumers.Result
  {
    public double sum;
    public double min;
    public double max;
    public long nElems;
    public MinMaxSumResult(double s, double _min, double _max, long _ne) {
      sum = s;
      min = _min;
      max = _max;
      nElems = _ne;
    }
    public long nElems() { return nElems; }
    public Object value() {
      HashMap retval = new HashMap();
      retval.put(Keyword.intern(null, "sum"), sum);
      retval.put(Keyword.intern(null, "min"), min);
      retval.put(Keyword.intern(null, "max"), max);
      return retval;
    }
    public Consumers.Result combine(Consumers.Result _other) {
      MinMaxSumResult other = (MinMaxSumResult)_other;
      return new MinMaxSumResult( sum + other.sum,
				  Math.min(min, other.min),
				  Math.max(max, other.max),
				  nElems + other.nElems );
    }
  }
  public static class Moments implements Consumers.StagedConsumer, DoubleConsumer
  {
    public final double mean;
    public double m2;
    public double m3;
    public double m4;
    public long nElems;
    public Moments(double _mean) {
      mean = _mean;
      m2 = 0.0;
      m3 = 0.0;
      m4 = 0.0;
      nElems = 0;
    }
    public void accept(double val) {
      double meanDiff = val - mean;
      double md2 = meanDiff * meanDiff;
      m2 += md2;
      double md3 = md2 * meanDiff;
      m3 += md3;
      double md4 = md3 * meanDiff;
      m4 += md4;
      nElems++;
    }
    public Consumers.Result result() {
      return new MomentsResult(m2,m3,m4,nElems);
    }
  }
  public static class MomentsResult implements Consumers.Result
  {
    public double m2;
    public double m3;
    public double m4;
    public long nElems;
    public MomentsResult(double _m2, double _m3, double _m4, long _ne) {
      m2 = _m2;
      m3 = _m3;
      m4 = _m4;
      nElems = _ne;
    }
    public long nElems() { return nElems; }
    public Object value() {
      HashMap retval = new HashMap();
      retval.put(Keyword.intern(null, "moment-2"), m2);
      retval.put(Keyword.intern(null, "moment-3"), m3);
      retval.put(Keyword.intern(null, "moment-4"), m4);
      return retval;
    }
    public Consumers.Result combine(Consumers.Result _other) {
      MomentsResult other = (MomentsResult)_other;
      return new MomentsResult( m2 + other.m2,
				m3 + other.m3,
				m4 + other.m4,
				nElems + other.nElems );
    }
  }
  //tight loop consume call.
  public static Consumers.Result consume(long offset, int grouplen, Buffer data,
					 Consumers.StagedConsumer consumer,
					 DoublePredicate predicate) {
    final DoubleConsumer dconsumer = (DoubleConsumer)consumer;
    if( predicate == null) {
      for (int idx = 0; idx < grouplen; ++idx) {
	dconsumer.accept(data.readDouble((long)idx + offset));
      }
    } else {
      for (int idx = 0; idx < grouplen; ++idx) {
	double dval = data.readDouble((long)idx + offset);
	if(predicate.test(dval)) {
	  dconsumer.accept(dval);
	}
      }
    }
    return consumer.result();
  }
}
