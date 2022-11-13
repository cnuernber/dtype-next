package tech.v3.datatype;

import java.util.function.DoublePredicate;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import ham_fisted.Casts;

public class UnaryPredicates
{
  public interface DoubleUnaryPredicate extends UnaryPredicate, DoublePredicate
  {
    default boolean unaryLong(long arg) {
      return unaryDouble((double)arg);
    }
    default boolean unaryObject(Object arg) {
      return unaryDouble(Casts.doubleCast(arg));
    }
    default boolean test(double arg) {
      return unaryDouble(arg);
    }
  }
  public interface LongUnaryPredicate extends UnaryPredicate, LongPredicate
  {
    default boolean unaryDouble(double arg) {
      return unaryLong(Casts.longCast(arg));
    }
    default boolean unaryObject(Object arg) {
      return unaryLong(Casts.longCast(arg));
    }
    default boolean test(long arg) {
      return unaryLong(arg);
    }
  }
  public interface ObjectUnaryPredicate extends UnaryPredicate, Predicate {
    default boolean test(Object arg) { return unaryObject(arg); }
  }
}
