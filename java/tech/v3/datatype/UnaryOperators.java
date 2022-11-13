package tech.v3.datatype;

import ham_fisted.Casts;

public class UnaryOperators
{
  public interface DoubleUnaryOperator extends UnaryOperator
  {
    default long unaryLong(long arg) {
      return Casts.longCast(unaryDouble((double)arg));
    }
    default Object unaryObject(Object arg) {
      return unaryDouble(Casts.doubleCast(arg));
    }
  }
  public interface LongUnaryOperator extends UnaryOperator
  {
    default double unaryDouble(double arg) {
      return (double)unaryLong(Casts.longCast(arg));
    }
    default Object unaryObject(Object arg) {
      return unaryLong(Casts.longCast(arg));
    }
  }
}
