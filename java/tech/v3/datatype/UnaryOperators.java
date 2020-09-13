package tech.v3.datatype;

import clojure.lang.RT;

public class UnaryOperators
{
  public interface BooleanUnaryOperator extends UnaryOperator
  {
    default byte unaryByte(byte arg) {
      return BooleanConversions.toByte(unaryBoolean( BooleanConversions.from(arg) ));
    }
    default short unaryShort(short arg) {
      return BooleanConversions.toShort(unaryBoolean( BooleanConversions.from(arg)));
    }
    default char unaryChar(char arg) {
      return BooleanConversions.toChar(unaryBoolean( BooleanConversions.from(arg)));
    }
    default int unaryInt(int arg) {
      return BooleanConversions.toInt(unaryBoolean( BooleanConversions.from(arg)));
    }
    default long unaryLong(long arg) {
      return BooleanConversions.toLong(unaryBoolean( BooleanConversions.from(arg)));
    }
    default float unaryFloat(float arg) {
      return BooleanConversions.toFloat(unaryBoolean( BooleanConversions.from(arg)));
    }
    default double unaryDouble(double arg) {
      return BooleanConversions.toDouble(unaryBoolean( BooleanConversions.from(arg)));
    }
    default Object unaryObject(Object arg) {
      return unaryBoolean(BooleanConversions.from(arg));
    }
  }
  public interface DoubleUnaryOperator extends UnaryOperator
  {
    default  boolean unaryBoolean(boolean arg) {
      return BooleanConversions.from(unaryDouble(BooleanConversions.toDouble(arg)));
    }
    default byte unaryByte(byte arg) {
      return RT.byteCast(unaryDouble((double)arg));
    }
    default short unaryShort(short arg) {
      return RT.shortCast(unaryDouble((double)arg));
    }
    default char unaryChar(char arg) {
      return RT.charCast(unaryDouble((double)arg));
    }
    default int unaryInt(int arg) {
      return RT.intCast(unaryDouble((double)arg));
    }
    default long unaryLong(long arg) {
      return RT.longCast(unaryDouble((double)arg));
    }
    default float unaryFloat(float arg) {
      return (float)unaryDouble((double)arg);
    }
    default Object unaryObject(Object arg) {
      return unaryDouble(RT.doubleCast(arg));
    }
  }
  public interface LongUnaryOperator extends UnaryOperator
  {
    default  boolean unaryBoolean(boolean arg) {
      return BooleanConversions.from(unaryLong(BooleanConversions.toLong(arg)));
    }
    default byte unaryByte(byte arg) {
      return RT.byteCast(unaryLong((long)arg));
    }
    default short unaryShort(short arg) {
      return RT.shortCast(unaryLong((long)arg));
    }
    default char unaryChar(char arg) {
      return RT.charCast(unaryLong((long)arg));
    }
    default int unaryInt(int arg) {
      return RT.intCast(unaryLong((long)arg));
    }
    default float unaryFloat(float arg) {
      return RT.floatCast(unaryLong((long)arg));
    }
    default double unaryDouble(double arg) {
      return (double)unaryLong((long)arg);
    }
    default Object unaryObject(Object arg) {
      return unaryLong(RT.longCast(arg));
    }
  }
  public interface ObjectUnaryOperator extends UnaryOperator
  {
    default  boolean unaryBoolean(boolean arg) {
      return BooleanConversions.from(unaryObject(arg));
    }
    default byte unaryByte(byte arg) {
      return RT.byteCast(unaryObject(arg));
    }
    default short unaryShort(short arg) {
      return RT.shortCast(unaryObject(arg));
    }
    default char unaryChar(char arg) {
      return RT.charCast(unaryObject(arg));
    }
    default int unaryInt(int arg) {
      return RT.intCast(unaryObject(arg));
    }
    default long unaryLong(long arg) {
      return RT.longCast(unaryObject(arg));
    }
    default float unaryFloat(float arg) {
      return RT.floatCast(unaryObject(arg));
    }
    default double unaryDouble(double arg) {
      return RT.doubleCast(unaryObject(arg));
    }
  }
}
