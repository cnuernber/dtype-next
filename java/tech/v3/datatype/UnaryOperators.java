package tech.v3.datatype;

import ham_fisted.Casts;
import ham_fisted.IFnDef;

public class UnaryOperators
{
  public interface DoubleUnaryOperator extends UnaryOperator, IFnDef.DD
  {
    default long unaryLong(long arg) {
      return Casts.longCast(unaryDouble(Casts.doubleCast(arg)));
    }
    default long unaryObjLong(Object arg) {
      return Casts.longCast(unaryDouble(Casts.doubleCast(arg)));
    }
    default double unaryObjDouble(Object arg) {
      return unaryDouble(Casts.doubleCast(arg));
    }
    default Object unaryObject(Object arg) {
      return unaryDouble(Casts.doubleCast(arg));
    }
    default Object invoke(Object arg) {
      return unaryDouble(Casts.doubleCast(arg));
    }
    default double invokePrim(double arg) {
      return unaryDouble(arg);
    }
  }
  public interface ObjDoubleUnaryOperator extends UnaryOperator, IFnDef.OD {
    default Object unaryObject(Object x) { return unaryObjDouble(x); }
    default double invokePrim(Object x) { return unaryObjDouble(x); }
    default Object invoke(Object x) { return unaryObjDouble(x); }
  }
  public interface LongUnaryOperator extends UnaryOperator, IFnDef.LL
  {
    default double unaryDouble(double arg) {
      return (double)unaryLong(Casts.longCast(arg));
    }
    default Object unaryObject(Object arg) {
      return unaryLong(Casts.longCast(arg));
    }
    default long unaryObjLong(Object arg) {
      return unaryLong(Casts.longCast(arg));
    }
    default double unaryObjDouble(Object arg) {
      return Casts.doubleCast(unaryLong(Casts.longCast(arg)));
    }
    default long invokePrim(long arg) {
      return unaryLong(arg);
    }
    default Object invoke(Object arg) {
      return unaryLong(Casts.longCast(arg));
    }
  }
  public interface ObjLongUnaryOperator extends UnaryOperator, IFnDef.OL {
    default Object unaryObject(Object x) { return unaryObjLong(x); }
    default long invokePrim(Object x) { return unaryObjLong(x); }
    default Object invoke(Object x) { return unaryObjLong(x); }
  }
}
