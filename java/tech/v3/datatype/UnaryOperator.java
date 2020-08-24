package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.ISeq;
import java.util.function.Function;


public interface UnaryOperator extends ElemwiseDatatype, IFn, Function
{
  boolean unaryBoolean(boolean arg);
  byte unaryByte(byte arg);
  short unaryShort(short arg);
  char unaryChar(char arg);
  int unaryInt(int arg);
  long unaryLong(long arg);
  float unaryFloat(float arg);
  double unaryDouble(double arg);
  Object unaryObject(Object arg);
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
}
