package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.ISeq;
import java.util.function.Function;
import ham_fisted.IFnDef;
import ham_fisted.Casts;


public interface UnaryOperator extends ElemwiseDatatype, IFnDef.LL, IFnDef.DD, IFnDef.OO
{

  Object unaryObject(Object arg);
  default long unaryLong(long arg) { return Casts.longCast(unaryObject(arg)); }
  default double unaryDouble(double arg) {
    return Casts.doubleCast(unaryObject(arg));
  }
  default long unaryObjLong(Object arg) { return Casts.longCast(unaryObject(arg)); }
  default double unaryObjDouble(Object arg) { return Casts.doubleCast(unaryObject(arg)); }
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object arg) { return unaryObject(arg); }
  default Object apply(Object arg) { return unaryObject(arg); }
  default long invokePrim(long arg) { return unaryLong(arg); }
  default double invokePrim(double arg) { return unaryDouble(arg); }
}
