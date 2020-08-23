package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.ISeq;
import java.util.function.Function;


public interface UnaryOperator extends ElemwiseDatatype, IFn, Function
{
  boolean unary(boolean arg);
  byte unary(byte arg);
  short unary(short arg);
  char unary(char arg);
  int unary(int arg);
  long unary(long arg);
  float unary(float arg);
  double unary(double arg);
  Object unary(Object arg);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object arg) { return unary(arg); }
  default Object applyTo(ISeq seq) {
    if (1 != seq.count()) {
      throw new RuntimeException("Argument count incorrect for unary op");
    }
    return invoke(seq.first());
  }
  default Object apply(Object arg) {
    return unary(arg);
  }
}
