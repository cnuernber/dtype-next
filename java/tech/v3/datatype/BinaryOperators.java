package tech.v3.datatype;

import ham_fisted.Casts;

public class BinaryOperators
{
  public interface DoubleBinaryOperator extends BinaryOperator
  {
    default long binaryLong(long lhs, long rhs) {
      return Casts.longCast(binaryDouble((double) lhs, (double) rhs));
    }
    default Object binaryObject(Object lhs, Object rhs) {
      return binaryDouble(Casts.doubleCast(lhs),Casts.doubleCast(rhs));
    }
  }
  public interface LongBinaryOperator extends BinaryOperator
  {
    default double binaryDouble(double lhs, double rhs) {
      return (double)binaryLong(Casts.longCast(lhs), Casts.longCast(rhs));
    }
    default Object binaryObject(Object lhs, Object rhs) {
      return binaryLong(Casts.longCast(lhs), Casts.longCast(rhs));
    }
  }
}
