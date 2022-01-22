package tech.v3.datatype;

import static tech.v3.Clj.*;
import static tech.v3.DType.*;
import clojure.lang.RT;
import clojure.lang.IFn;
import java.util.Map;

/**
 *  Simple NaN-aware statstical methods.
 */
public class Stats {
  private Stats() {}

  static final IFn descStatsFn = requiringResolve("tech.v3.datatype.statistics",
					      "descriptive-statistics");
  static final IFn sumFn = requiringResolve("tech.v3.datatype.statistics", "sum");
  static final IFn minFn = requiringResolve("tech.v3.datatype.statistics", "min");
  static final IFn maxFn = requiringResolve("tech.v3.datatype.statistics", "max");
  static final IFn meanFn = requiringResolve("tech.v3.datatype.statistics", "mean");
  static final IFn medianFn = requiringResolve("tech.v3.datatype.statistics", "median");
  static final IFn varianceFn = requiringResolve("tech.v3.datatype.statistics", "variance");
  static final IFn stddevFn = requiringResolve("tech.v3.datatype.statistics", "standard-deviation");
  static final IFn skewFn = requiringResolve("tech.v3.datatype.statistics", "skew");
  static final IFn kurtosisFn = requiringResolve("tech.v3.datatype.statistics", "kurtosis");
  static final IFn pearsonsCorrFn = requiringResolve("tech.v3.datatype.statistics",
						 "pearsons-correlation");
  static final IFn spearmansCorrFn = requiringResolve("tech.v3.datatype.statistics",
						  "spearmans-correlation");
  static final IFn kendallsCorrFn = requiringResolve("tech.v3.datatype.statistics",
						 "kendalls-correlation");
  static final IFn percentilesFn = requiringResolve("tech.v3.datatype.statistics",
						"percentiles");
  static final IFn quartilesFn = requiringResolve("tech.v3.datatype.statistics",
						  "quartiles");

  /**
   * <p>Return a map of descriptive stat name to statistic. Statistic names are
   * described with keywords.  All stats methods are NaN aware meaning
   * nan's are removed before calculation.e</p>
   *
   * <p> Available stats:</p>
   * <pre>
   * [:min :quartile-1 :sum :mean :mode :median :quartile-3 :max
   *  :variance :standard-deviation :skew :n-values :kurtosis} </pre>
   */
  public static Map descriptiveStatistics(Object statsNames, Object data) {
    return (Map)call(descStatsFn, statsNames, data);
  }
  /**
   * Create a reader of percentile values, one for each percentage passed in.
   */
  public static Buffer percentiles(Object percentages, Object data) {
    return toBuffer(call(percentilesFn, percentages, data));
  }
  /**
   * @return <pre>percentiles(vector(min, 25, 50, 75, max), item)</pre>.
   */
  public static Buffer quartiles(Object data) {
    return toBuffer(call(quartilesFn, data));
  }

  public static double min(Object data) {
    return (double)call(minFn, data);
  }
  public static double max(Object data) {
    return (double)call(maxFn, data);
  }
  /**
   *  High quality parallelized summation using kahas compensation.
   */
  public static double sum(Object data) {
    return (double)call(sumFn, data);
  }
  /**
   *  High quality parallelized mean using kahas compensation.
   */
  public static double mean(Object data) {
    return (double)call(meanFn, data);
  }
  public static double median(Object data) {
    return (double)call(medianFn, data);
  }
  public static double variance(Object data) {
    return (double)call(varianceFn, data);
  }
  public static double stddev(Object data) {
    return (double)call(stddevFn, data);
  }
  public static double skew(Object data) {
    return (double)call(skewFn, data);
  }
  public static double kurtosis(Object data) {
    return (double)call(kurtosisFn, data);
  }
  public static double kendallsCorrelation(Object lhs, Object rhs) {
    return (double)call(kendallsCorrFn, lhs, rhs);
  }
  public static double spearmansCorrelation(Object lhs, Object rhs) {
    return (double)call(spearmansCorrFn, lhs, rhs);
  }
  public static double pearsonsCorrelation(Object lhs, Object rhs) {
    return (double)call(pearsonsCorrFn, lhs, rhs);
  }

}
