package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.ISeq;
import java.util.function.BiFunction;
import java.util.function.DoubleBinaryOperator;


public interface BinaryOperator extends ElemwiseDatatype, IFn, BiFunction, DoubleBinaryOperator
{
  boolean binaryBoolean(boolean lhs, boolean rhs);
  byte binaryByte(byte lhs, byte rhs);
  short binaryShort(short lhs, short rhs);
  char binaryChar(char lhs, char rhs);
  int binaryInt(int lhs, int rhs);
  long binaryLong(long lhs, long rhs);
  float binaryFloat(float lhs, float rhs);
  double binaryDouble(double lhs, double rhs);
  Object binaryObject(Object lhs, Object rhs);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object lhs, Object rhs) { return binaryObject(lhs,rhs); }
  default Object applyTo(ISeq seq) {
    if (2 != seq.count()) {
      throw new RuntimeException("Argument count incorrect for binary op");
    }
    return binaryObject(seq.first(), seq.next().first());
  }
  default Object apply(Object lhs, Object rhs) {
    return binaryObject(lhs, rhs);
  }
  default double applyAsDouble(double left, double right) {
    return binaryDouble(left, right);
  }
  default double initialDoubleReductionValue() { return 0.0; }
}
