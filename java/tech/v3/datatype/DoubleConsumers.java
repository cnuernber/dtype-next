package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.function.DoubleConsumer;
import ham_fisted.Sum;
import ham_fisted.Reducible;
import ham_fisted.MutArrayMap;
import ham_fisted.BitmapTrieCommon;
import clojure.lang.IDeref;


public class DoubleConsumers
{
  public static final class MinMaxSum implements Reducible, DoubleConsumer, IDeref
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

    public Reducible reduce(Reducible _other) {
      MinMaxSum other = (MinMaxSum)_other;
      if (sum.nElems == 0)
	return _other;
      else if (other.sum.nElems == 0)
	return this;
      else
	return new MinMaxSum((Sum)sum.reduce(other.sum),
			     Math.min(getMin(), other.getMin()),
			     Math.max(getMax(), other.getMax()));
    }

    public Object deref() {
      return MutArrayMap.createKV(BitmapTrieCommon.defaultHashProvider,
				  Sum.sumKwd, sum.computeFinalSum(),
				  sum.nElemsKwd, sum.nElems,
				  minKwd, getMin(),
				  maxKwd, getMax()).persistent();
    }
  }

  public static class Moments implements Reducible, DoubleConsumer, IDeref
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
    public Reducible reduce(Reducible _other) {
      Moments other = (Moments)_other;
      Moments retval = new Moments(mean);
      retval.m2 = m2 + other.m2;
      retval.m3 = m3 + other.m3;
      retval.m4 = m4 + other.m4;
      retval.nElems = nElems + other.nElems;
      return retval;
    }
    public Object deref() {
      return MutArrayMap.createKV(BitmapTrieCommon.defaultHashProvider,
				  m2Kwd, m2,
				  m3Kwd, m3,
				  m4Kwd, m4,
				  Sum.nElemsKwd, nElems).persistent();
    }
  }
}
