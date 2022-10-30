package tech.v3.datatype;


import clojure.lang.Keyword;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.DoublePredicate;
import java.util.function.LongPredicate;
import ham_fisted.IFnDef;


public interface UnaryPredicate extends ElemwiseDatatype, IFnDef.LO, IFnDef.DO,
					Predicate
{
  default boolean unaryLong(long arg) { return unaryObject(arg); }
  default boolean unaryDouble(double arg) { return unaryObject(arg); }
  boolean unaryObject(Object arg);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object arg) { return unaryObject(arg); }
  default boolean test(Object arg) { return unaryObject(arg); }
}
