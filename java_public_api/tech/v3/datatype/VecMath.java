package tech.v3.datatype;



import static tech.v3.Clj.*;
import static tech.v3.DType.*;
import clojure.lang.RT;
import clojure.lang.IFn;
import tech.v3.datatype.BooleanReader;

/**
 * <p>Functional lazy math abstraction built for performing elemwise mathematical
 * operations on scalars and buffers.</p>
 *
 * <p>Arithmetic and statistical operations based on the Buffer interface. These
 * operators and functions all implement vectorized interfaces so passing in
 * something convertible to a reader will return a reader. Arithmetic operations are
 * done lazily. These functions generally incur a large dispatch cost so for example
 * each call to '+' checks all the arguments to decide if it should dispatch to an
 * iterable implementation or to a reader implementation. For tight loops or
 * operations like map and filter, using the specific operators will result in far
 * faster code than using the 'add' function itself.</p>
 *
 * <p>It is important to note that in generla these functions are typed such that if they take
 * a vector of numbers, they return a vector of numbers.  For example, <pre>max([2,3,4],3)</pre>
 * returns <pre>[3,3,4]</pre>.  Functions that are typed to return scalars are prefixed
 * with reduce so <pre>reduceMax([2,3,4])</pre> returns 4.</p>
 */
public class VecMath
{
  static final IFn booleansToIndexesFn = requiringResolve("tech.v3.datatype.unary-pred", "bool-reader->indexes");

  static final IFn addFn = requiringResolve("tech.v3.datatype.functional", "+");
  static final IFn subFn = requiringResolve("tech.v3.datatype.functional", "-");
  static final IFn mulFn = requiringResolve("tech.v3.datatype.functional", "*");
  static final IFn divFn = requiringResolve("tech.v3.datatype.functional", "/");
  static final IFn sqFn = requiringResolve("tech.v3.datatype.functional", "sq");
  static final IFn sqrtFn = requiringResolve("tech.v3.datatype.functional", "sqrt");
  static final IFn cbrtFn = requiringResolve("tech.v3.datatype.functional", "cbrt");
  static final IFn absFn = requiringResolve("tech.v3.datatype.functional", "abs");
  static final IFn powFn = requiringResolve("tech.v3.datatype.functional", "pow");
  static final IFn quotFn = requiringResolve("tech.v3.datatype.functional", "quot");
  static final IFn remFn = requiringResolve("tech.v3.datatype.functional", "rem");


  static final IFn cosFn = requiringResolve("tech.v3.datatype.functional", "cos");
  static final IFn coshFn = requiringResolve("tech.v3.datatype.functional", "cosh");
  static final IFn acosFn = requiringResolve("tech.v3.datatype.functional", "acos");
  static final IFn sinFn = requiringResolve("tech.v3.datatype.functional", "sin");
  static final IFn sinhFn = requiringResolve("tech.v3.datatype.functional", "sinh");
  static final IFn asinFn = requiringResolve("tech.v3.datatype.functional", "asin");
  static final IFn tanFn = requiringResolve("tech.v3.datatype.functional", "tan");
  static final IFn tanhFn = requiringResolve("tech.v3.datatype.functional", "tanh");
  static final IFn atanFn = requiringResolve("tech.v3.datatype.functional", "atan");
  static final IFn hypotFn = requiringResolve("tech.v3.datatype.functional", "hypot");
  static final IFn toRadFn = requiringResolve("tech.v3.datatype.functional", "to-radians");
  static final IFn toDegFn = requiringResolve("tech.v3.datatype.functional", "to-degrees");

  static final IFn finiteFn = requiringResolve("tech.v3.datatype.functional", "finite?");
  static final IFn infiniteFn = requiringResolve("tech.v3.datatype.functional", "infinite?");
  static final IFn nanFn = requiringResolve("tech.v3.datatype.functional", "nan?");
  static final IFn ceilFn = requiringResolve("tech.v3.datatype.functional", "ceil");
  static final IFn floorFn = requiringResolve("tech.v3.datatype.functional", "floor");
  static final IFn significandFn = requiringResolve("tech.v3.datatype.functional", "get-significand");
  static final IFn ieeeRemainderFn = requiringResolve("tech.v3.datatype.functional", "ieee-remainder");
  static final IFn mathematicalIntegerFn = requiringResolve("tech.v3.datatype.functional", "mathematical-integer?");
  static final IFn roundFn = requiringResolve("tech.v3.datatype.functional", "round");


  static final IFn ltFn = requiringResolve("tech.v3.datatype.functional", "<");
  static final IFn lteFn = requiringResolve("tech.v3.datatype.functional", "<=");
  static final IFn gtFn = requiringResolve("tech.v3.datatype.functional", ">");
  static final IFn gteFn = requiringResolve("tech.v3.datatype.functional", ">=");
  static final IFn eqFn = requiringResolve("tech.v3.datatype.functional", "eq");
  static final IFn notEqFn = requiringResolve("tech.v3.datatype.functional", "not-eq");
  static final IFn notFn = requiringResolve("tech.v3.datatype.functional", "not");


  static final IFn maxFn = requiringResolve("tech.v3.datatype.functional", "max");
  static final IFn reduceMaxFn = requiringResolve("tech.v3.datatype.functional", "reduce-max");
  static final IFn minFn = requiringResolve("tech.v3.datatype.functional", "min");
  static final IFn reduceMinFn = requiringResolve("tech.v3.datatype.functional", "reduce-min");


  static final IFn expFn = requiringResolve("tech.v3.datatype.functional", "exp");
  static final IFn expm1Fn = requiringResolve("tech.v3.datatype.functional", "expm1");
  static final IFn logFn = requiringResolve("tech.v3.datatype.functional", "log");
  static final IFn log1pFn = requiringResolve("tech.v3.datatype.functional", "log1p");
  static final IFn logisticFn = requiringResolve("tech.v3.datatype.functional", "logistic");


  static final IFn sumFn = requiringResolve("tech.v3.datatype.functional", "sum");
  static final IFn sumFastFn = requiringResolve("tech.v3.datatype.functional", "sum-fast");
  static final IFn meanFn = requiringResolve("tech.v3.datatype.functional", "sum");
  static final IFn meanFastFn = requiringResolve("tech.v3.datatype.functional", "mean-fast");
  static final IFn distanceFn = requiringResolve("tech.v3.datatype.functional", "distance");
  static final IFn distanceSqFn = requiringResolve("tech.v3.datatype.functional", "distance-squared");
  static final IFn magnitudeFn = requiringResolve("tech.v3.datatype.functional", "magnitude");
  static final IFn magnitudeSqFn = requiringResolve("tech.v3.datatype.functional", "magnitude-squared");
  static final IFn dotProdFn = requiringResolve("tech.v3.datatype.functional", "dot-product");

  static final IFn cumminFn = requiringResolve("tech.v3.datatype.functional", "cummin");
  static final IFn cummaxFn = requiringResolve("tech.v3.datatype.functional", "cummax");
  static final IFn cumsumFn = requiringResolve("tech.v3.datatype.functional", "cumsum");
  static final IFn cumprodFn = requiringResolve("tech.v3.datatype.functional", "cumprod");

  static final IFn equalsFn = requiringResolve("tech.v3.datatype.functional", "equals");


  private VecMath(){}

  /**
   * Efficiently convert a Buffer of boolean values to indexes that indicate
   * where the true values are.  After which you can reindex your buffer leaving
   * only the values where the condition was true.
   *
   * @see tech.v3.Dtype.indexedBuffer.
   */
  public static Object booleansToIndexes(Object boolVec) {
    return call(booleansToIndexesFn, boolVec);
  }

  /**
   * Given a IFn that returns boolean values, reindex lhs where elemwiseComp returned
   * true.  If elemwiseComp is an IFn, elemwiseComp is passed each element in lhs
   * else elemwiseComp is interpreted as a boolean buffer.
   *
   * @return indexes where condition is true.
   */
  public static Object indexesWhere(Object lhs, Object elemwiseComp) {
    Buffer lhsBuf = toBuffer(lhs);
    Buffer rdr = null;
    if (elemwiseComp instanceof IFn) {
      IFn compFn = (IFn)elemwiseComp;
      rdr = new BooleanReader() {
	  public long lsize() { return lhsBuf.lsize(); }
	  public boolean readBoolean(long idx) {
	    return boolCast(compFn.invoke(lhsBuf.readObject(idx)));
	  }
	};
    } else {
      rdr = toBuffer(elemwiseComp);
      if(lhsBuf.lsize() != rdr.lsize()) {
	throw new RuntimeException("Input/booleanvec size mismatch: expected "
				   + String.valueOf(lhsBuf.lsize()) + ", got "
				   + String.valueOf(rdr.lsize()));
      }
    }
    return booleansToIndexes(rdr);
  }

  /**
   * Given a IFn that returns boolean values, reindex lhs where elemwiseComp returned
   * true.  If elemwiseComp is an IFn, elemwiseComp is passed each element in lhs
   * else elemwiseComp is interpreted as a boolean buffer.
   *
   * @return - a possibly much shorter lhs.
   */
  public static Buffer where(Object lhs, Object elemwiseComp) {
    return indexedBuffer(indexesWhere(lhs, elemwiseComp), lhs);
  }

  public static Object add(Object lhs, Object rhs) {
    return call(addFn, lhs, rhs);
  }
  public static Object neg(Object lhs) {
    return call(subFn, lhs);
  }
  public static Object sub(Object lhs, Object rhs) {
    return call(subFn, lhs, rhs);
  }
  public static Object mul(Object lhs, Object rhs) {
    return call(mulFn, lhs, rhs);
  }
  public static Object div(Object lhs, Object rhs) {
    return call(divFn, lhs, rhs);
  }
  /**
   * Square each element of lhs.
   */
  public static Object sq(Object lhs) {
    return call(sqFn, lhs);
  }
  public static Object sqrt(Object lhs) {
    return call(sqrtFn, lhs);
  }
  /**
   * Cube root of each element.
   */
  public static Object cbrt(Object lhs) {
    return call(cbrtFn, lhs);
  }
  public static Object abs(Object lhs) {
    return call(absFn, lhs);
  }
  public static Object pow(Object lhs) {
    return call(powFn, lhs);
  }
  /**
   * Integer divide.
   */
  public static Object quot(Object lhs) {
    return call(quotFn, lhs);
  }
  /**
   * Integer remainder
   */
  public static Object rem(Object lhs) {
    return call(remFn, lhs);
  }

  public static Object cos(Object lhs) {
    return call(cosFn, lhs);
  }
  public static Object acos(Object lhs) {
    return call(acosFn, lhs);
  }
  public static Object cosh(Object lhs) {
    return call(coshFn, lhs);
  }
  public static Object sin(Object lhs) {
    return call(sinFn, lhs);
  }
  public static Object asin(Object lhs) {
    return call(asinFn, lhs);
  }
  public static Object sinh(Object lhs) {
    return call(sinhFn, lhs);
  }
  public static Object tan(Object lhs) {
    return call(tanFn, lhs);
  }
  public static Object atan(Object lhs) {
    return call(atanFn, lhs);
  }
  public static Object tanh(Object lhs) {
    return call(tanhFn, lhs);
  }
  public static Object toRadians(Object lhs) {
    return call(toRadFn, lhs);
  }
  public static Object toDegrees(Object lhs) {
    return call(toDegFn, lhs);
  }

  public static Object isFinite(Object lhs) {
    return call(finiteFn, lhs);
  }
  public static Object isInfinite(Object lhs) {
    return call(infiniteFn, lhs);
  }
  public static Object isNan(Object lhs) {
    return call(nanFn, lhs);
  }
  public static Object ceil(Object lhs) {
    return call(ceilFn, lhs);
  }
  public static Object floor(Object lhs) {
    return call(floorFn, lhs);
  }
  public static Object round(Object lhs) {
    return call(roundFn, lhs);
  }
  public static Object significand(Object lhs) {
    return call(significandFn, lhs);
  }
  public static Object IEEERemainder(Object lhs) {
    return call(ieeeRemainderFn, lhs);
  }
  public static Object isMathematicalInteger(Object lhs) {
    return call(mathematicalIntegerFn, lhs);
  }


  public static Object lessThan(Object lhs, Object rhs) {
    return call(ltFn, lhs, rhs);
  }
  public static Object lessThan(Object lhs, Object mid, Object rhs) {
    return call(ltFn, lhs, mid, rhs);
  }
  public static Object lessThanOrEq(Object lhs, Object rhs) {
    return call(lteFn, lhs, rhs);
  }
  public static Object lessThanOrEq(Object lhs, Object mid, Object rhs) {
    return call(lteFn, lhs, mid, rhs);
  }
  public static Object greaterThan(Object lhs, Object rhs) {
    return call(ltFn, lhs, rhs);
  }
  public static Object greaterThan(Object lhs, Object mid, Object rhs) {
    return call(ltFn, lhs, mid, rhs);
  }
  public static Object greaterThanOrEq(Object lhs, Object rhs) {
    return call(lteFn, lhs, rhs);
  }
  public static Object greaterThanOrEq(Object lhs, Object mid, Object rhs) {
    return call(lteFn, lhs, mid, rhs);
  }
  /**
   * Nan-aware eq operation returning buffer of booleans
   */
  public static Object eq(Object lhs, Object rhs) {
    return call(eqFn, lhs, rhs);
  }
  public static Object notEq(Object lhs, Object rhs) {
    return call(notEqFn, lhs, rhs);
  }
  /**
   * Return complement of vector of booleans.
   */
  public static Object not(Object lhs) {
    return call(notFn, lhs);
  }
  public static Object max(Object lhs, Object rhs) {
    return call(maxFn, lhs, rhs);
  }
  public static Object reduceMax(Object lhs) {
    return call(reduceMaxFn, lhs);
  }
  public static Object min(Object lhs, Object rhs) {
    return call(minFn, lhs, rhs);
  }
  public static Object reduceMin(Object lhs) {
    return call(reduceMinFn, lhs);
  }

  public static Object exp(Object lhs) {
    return call(expFn, lhs);
  }
  public static Object expm1(Object lhs) {
    return call(expm1Fn, lhs);
  }
  public static Object log(Object lhs) {
    return call(logFn, lhs);
  }
  public static Object log1p(Object lhs) {
    return call(log1pFn, lhs);
  }
  public static Object logistic(Object lhs) {
    return call(logisticFn, lhs);
  }

  /**
   * High quality summation - drops NaN values and uses Kahan's compensated algorithm.
   *
   * @see tech.v3.datatype.Stats
   */
  public static double sum(Object data) {
    return (double)call(sumFn, data);
  }
  /**
   * Fast parallel summation.
   */
  public static double sumFast(Object data) {
    return (double)call(sumFastFn, data);
  }
  /**
   *  High quality mean - drops NaN values and uses Kahan's compensation for summation.
   *
   * @see tech.v3.datatype.Stats
   */
  public static double mean(Object data) {
    return (double)call(meanFn, data);
  }
  /**
   * Fast parallel naive mean.
   */
  public static double meanFast(Object data) {
    return (double)call(meanFastFn, data);
  }
  public static double distance(Object lhs, Object rhs) {
    return (double)call(distanceFn, lhs, rhs);
  }
  public static double distanceSquared(Object lhs, Object rhs) {
    return (double)call(distanceSqFn, lhs, rhs);
  }
  public static double magnitude(Object lhs) {
    return (double)call(magnitudeFn, lhs);
  }
  public static double magnitudeSquared(Object lhs) {
    return (double)call(magnitudeSqFn, lhs);
  }
  public static double dotProduct(Object lhs, Object rhs) {
    return (double)call(dotProdFn, lhs, rhs);
  }

  public static Object cummin(Object lhs) {
    return call(cumminFn, lhs);
  }
  public static Object cummax(Object lhs) {
    return call(cummaxFn, lhs);
  }
  public static Object cumsum(Object lhs) {
    return call(cumsumFn, lhs);
  }
  public static Object cumprod(Object lhs) {
    return call(cumprodFn, lhs);
  }
  /**
   * Return true if the distance between lhs and rhs is less than epsilon.
   */
  public static boolean isEqual(Object lhs, Object rhs, double epsilon) {
    return (boolean)call(equalsFn, lhs, rhs, epsilon);
  }
  /**
   * Return true if the distance between lhs and rhs is less than 0.001.
   */
  public static boolean isEqual(Object lhs, Object rhs) {
    return isEqual(lhs,rhs,0.001);
  }
};
