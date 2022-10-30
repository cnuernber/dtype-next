package tech.v3.datatype;

import clojure.lang.Keyword;
import ham_fisted.IFnDef;
import ham_fisted.Casts;


public interface BinaryOperator extends ElemwiseDatatype, IFnDef.OOO, IFnDef.LLL, IFnDef.DDD
{
  default long binaryLong(long lhs, long rhs) { return Casts.longCast(binaryObject(lhs,rhs)); }
  default double binaryDouble(double lhs, double rhs) { return Casts.doubleCast(binaryObject(lhs,rhs)); }
  Object binaryObject(Object lhs, Object rhs);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object lhs, Object rhs) { return binaryObject(lhs,rhs); }
  default double invokePrim(double left, double right) {
    return binaryDouble(left, right);
  }
  default long invokePrim(long left, long right) {
    return binaryLong(left, right);
  }
  default double initialDoubleReductionValue() { return 0.0; }
  default long initialLongReductionValue() { return 0; }
}
