package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.function.DoubleConsumer;
import java.util.function.DoublePredicate;
import java.util.HashMap;
import java.util.Collections;


public class DoubleConsumers
{
  public static abstract class ScalarReduceBase implements Consumers.StagedConsumer,
							   DoubleConsumer
  {
    public static final Keyword nElemsKwd = Keyword.intern(null, "n-elems");
    public double value;
    public long nElems;
    public ScalarReduceBase() {
      value = 0.0;
      nElems = 0;
    }
    public ScalarReduceBase(double _value, long _nElems) {
      value = _value;
      nElems = _nElems;
    }
    public static Object valueMap(Keyword valueKwd, double value, long nElems ){
      HashMap hm = new HashMap();
      hm.put( valueKwd, value );
      hm.put( nElemsKwd, nElems );
      return hm;
    }
  }
  //High quality summation with code taken from Collectors.
  //Implements
  public static final class Sum implements Consumers.StagedConsumer,
					   DoubleConsumer
  {
    //Low order summation bits
    public double d0;
    //High order summation bits
    public double d1;
    public double simpleSum;
    public long nElems;
    public static final Keyword valueKwd = Keyword.intern(null, "sum");
    /**
     * Incorporate a new double value using Kahan summation /
     * compensation summation.
     *
     * High-order bits of the sum are in intermediateSum[0], low-order
     * bits of the sum are in intermediateSum[1], any additional
     * elements are application-specific.
     *
     * @param intermediateSum the high-order and low-order words of the intermediate sum
     * @param value the name value to be included in the running sum
     */
    public void sumWithCompensation(double value) {
      double tmp = value - d1;
      double sum = d0;
      double velvel = sum + tmp; // Little wolf of rounding error
      d1 =  (velvel - sum) - tmp;
      d0 = velvel;
    }

    public double computeFinalSum() {
      // Better error bounds to add both terms as the final sum
      double tmp = d0 + d1;
      if (Double.isNaN(tmp) && Double.isInfinite(simpleSum))
	return simpleSum;
      else
	return tmp;
    }

    public Sum(double _d0, double _d1, double _simpleSum, long _nElems) {
      d0 = _d0;
      d1 = _d1;
      simpleSum = _simpleSum;
      nElems = _nElems;
    }
    public Sum() {
      this(0, 0, 0, 0);
    }
    public void accept(double data) {
      sumWithCompensation(data);
      simpleSum += data;
      nElems++;
    }

    public Consumers.StagedConsumer combine(Consumers.StagedConsumer _other) {
      Sum other = (Sum)_other;
      Sum accum = new Sum(d0, d1, simpleSum, nElems);
      accum.sumWithCompensation(other.d0);
      accum.sumWithCompensation(other.d1);
      accum.simpleSum += other.simpleSum;
      accum.nElems += other.nElems;
      return accum;
    }

    public Object value() {
      return ScalarReduceBase.valueMap(valueKwd,computeFinalSum(),nElems);
    }
  }
  public static class UnaryOpSum implements Consumers.StagedConsumer,
					    DoubleConsumer
  {
    public final UnaryOperator op;
    public final Sum sum;
    public UnaryOpSum(UnaryOperator _op, Sum _sum) {
      op = _op;
      sum = _sum;
    }
    public UnaryOpSum(UnaryOperator _op) {
      this(_op, new Sum());
    }
    public void accept(double data) {
      sum.accept(op.unaryDouble(data));
    }
    public Consumers.StagedConsumer combine(Consumers.StagedConsumer _other) {
      return new UnaryOpSum(op, (Sum)sum.combine(((UnaryOpSum)_other).sum));
    }
    public Object value() {
      return sum.value();
    }
  }
  public static class BinaryOp extends ScalarReduceBase
  {
    public static final Keyword defaultValueKwd = Keyword.intern(null, "value");
    public final BinaryOperator op;
    public final Keyword valueKwd;
    public BinaryOp(BinaryOperator _op, double initValue, long initnElems, Keyword _valueKwd) {
      super(initValue, initnElems);
      op = _op;
      if (_valueKwd == null) {
	valueKwd = defaultValueKwd;
      } else {
	valueKwd = _valueKwd;
      }
    }
    public BinaryOp(BinaryOperator _op, double initValue) {
      this(_op, initValue, 0, null);
    }
    public BinaryOp(BinaryOperator _op, double initValue, long nElems) {
      this(_op, initValue, nElems, null);
    }
    public void accept(double data) {
      value = op.binaryDouble(value, data);
      nElems++;
    }
    public Consumers.StagedConsumer combine(Consumers.StagedConsumer _other) {
      BinaryOp other = (BinaryOp)_other;
      return new BinaryOp(op, op.binaryDouble(value, other.value),
			  nElems + other.nElems, valueKwd);
    }
    public Object value() {
      return valueMap(valueKwd,value,nElems);
    }
  }
  public static final class MinMaxSum implements Consumers.StagedConsumer, DoubleConsumer
  {
    public static final Keyword minKwd = Keyword.intern(null, "min");
    public static final Keyword maxKwd = Keyword.intern(null, "max");
    public Sum sum;
    public double min;
    public double max;
    public MinMaxSum(Sum _sum, double _min, double _max) {
      sum = _sum;
      max = _max;
      min = _min;
    }
    public MinMaxSum() {
      this(new Sum(), Double.MAX_VALUE, -Double.MAX_VALUE );
    }
    public void accept(double val) {
      sum.accept(val);
      min = Math.min(val, min);
      max = Math.max(val, max);
    }
    double getMin() {
      if (sum.nElems == 0)
	return Double.NaN;
      return min;
    }

    double getMax() {
      if (sum.nElems == 0)
	return Double.NaN;
      return max;
    }

    public Consumers.StagedConsumer combine(Consumers.StagedConsumer _other) {
      MinMaxSum other = (MinMaxSum)_other;
      return new MinMaxSum((Sum)sum.combine(other.sum),
			   Math.min(getMin(), other.getMin()),
			   Math.max(getMax(), other.getMax()));
    }

    public Object value() {
      HashMap retval = (HashMap)sum.value();
      retval.put(minKwd, getMin());
      retval.put(maxKwd, getMax());
      return retval;
    }
  }
  //Numerically inaccurate method for calculating moments but fairly unused.
  public static class Moments implements Consumers.StagedConsumer, DoubleConsumer
  {
    public static final Keyword m2Kwd = Keyword.intern(null, "moment-2");
    public static final Keyword m3Kwd = Keyword.intern(null, "moment-3");
    public static final Keyword m4Kwd = Keyword.intern(null, "moment-4");
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
    //Squaring them like this is unstable but OK for now.
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
    public Consumers.StagedConsumer combine(Consumers.StagedConsumer _other) {
      Moments other = (Moments)_other;
      Moments retval = new Moments(mean);
      retval.m2 = m2 + other.m2;
      retval.m3 = m3 + other.m3;
      retval.m4 = m4 + other.m4;
      retval.nElems = nElems + other.nElems;
      return retval;
    }
    public Object value() {
      HashMap retval = new HashMap();
      retval.put(m2Kwd, m2);
      retval.put(m3Kwd, m3);
      retval.put(m4Kwd, m4);
      retval.put(ScalarReduceBase.nElemsKwd, nElems);
      return retval;
    }
  }
  public static Consumers.StagedConsumer
    consume(long offset, int grouplen, Buffer data,
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
    return consumer;
  }
}
