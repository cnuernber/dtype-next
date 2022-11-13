package tech.v3.datatype;

import ham_fisted.Casts;

public class BinaryPredicates
{
  public interface DoubleBinaryPredicate extends BinaryPredicate
  {
    default boolean binaryLong(long lhs, long rhs) {
      return binaryDouble((double)lhs,(double)rhs);
    }
    default boolean binaryObject(Object lhs, Object rhs) {
      return binaryDouble(Casts.doubleCast(lhs),Casts.doubleCast(lhs));
    }
  }
  public interface LongBinaryPredicate extends BinaryPredicate
  {
    default boolean binaryDouble(double lhs, double rhs) {
      return binaryLong(Casts.longCast(lhs),Casts.longCast(rhs));
    }
    default boolean binaryObject(Object lhs, Object rhs) {
      return binaryLong(Casts.longCast(lhs),Casts.longCast(rhs));
    }
  }
}
