package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.ISeq;
import java.util.function.BiFunction;


public interface BinaryOperator extends ElemwiseDatatype, IFn, BiFunction
{
  boolean binary(boolean lhs, boolean rhs);
  byte binary(byte lhs, byte rhs);
  short binary(short lhs, short rhs);
  char binary(char lhs, char rhs);
  int binary(int lhs, int rhs);
  long binary(long lhs, long rhs);
  float binary(float lhs, float rhs);
  double binary(double lhs, double rhs);
  Object binary(Object lhs, Object rhs);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object lhs, Object rhs) { return binary(lhs,rhs); }
  default Object applyTo(ISeq seq) {
    if (2 != seq.count()) {
      throw new RuntimeException("Argument count incorrect for binary op");
    }
    return binary(seq.first(), seq.next().first());
  }
  default Object apply(Object lhs, Object rhs) {
    return binary(lhs, rhs);
  }
}
