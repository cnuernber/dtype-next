package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.ISeq;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.DoublePredicate;


public interface UnaryPredicate extends ElemwiseDatatype, IFnDef, Function,
					DoublePredicate
{
  boolean unaryBoolean(boolean arg);
  boolean unaryByte(byte arg);
  boolean unaryShort(short arg);
  boolean unaryChar(char arg);
  boolean unaryInt(int arg);
  boolean unaryLong(long arg);
  boolean unaryFloat(float arg);
  boolean unaryDouble(double arg);
  boolean unaryObject(Object arg);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object arg) { return unaryObject(arg); }
  default Object applyTo(ISeq seq) {
    if (1 != seq.count()) {
      throw new RuntimeException("Argument count incorrect for unary op");
    }
    return invoke(seq.first());
  }
  default Object apply(Object arg) {
    return unaryObject(arg);
  }
  default boolean test(Object arg) {
    return unaryObject(arg);
  }
  default boolean test(double arg) {
    return unaryDouble(arg);
  }
}
