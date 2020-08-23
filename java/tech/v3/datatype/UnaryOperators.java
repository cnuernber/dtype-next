package tech.v3.datatype;

import clojure.lang.RT;

public class UnaryOperators {
  public interface BooleanUnary extends UnaryOperator
  {
    boolean op(boolean arg);
    default boolean unary(boolean arg) {
      return op(arg);
    }
    default byte unary(byte arg) { return BooleanConversions.toByte(op(BooleanConversions.from(arg))); }
    default short unary(short arg)  { return BooleanConversions.toShort(op(BooleanConversions.from(arg))); }
    default char unary(char arg)  { return BooleanConversions.toChar(op(BooleanConversions.from(arg))); }
    default int unary(int arg)  { return BooleanConversions.toInt(op(BooleanConversions.from(arg))); }
    default long unary(long arg)  { return BooleanConversions.toLong(op(BooleanConversions.from(arg))); }
    default float unary(float arg)  { return BooleanConversions.toFloat(op(BooleanConversions.from(arg))); }
    default double unary(double arg)  { return BooleanConversions.toDouble(op(BooleanConversions.from(arg))); }
    default Object unary(Object arg)  { return op(BooleanConversions.from(arg)); }
  };
  public interface ByteUnary extends UnaryOperator
  {
    byte op(byte arg);
    default boolean unary(boolean arg) {
      return BooleanConversions.from(op(BooleanConversions.toByte(arg)));
    }
    default byte unary(byte arg) { return op(arg); }
    default short unary(short arg) { return (short) op(RT.byteCast(arg)); }
    default char unary(char arg) { return (char) op(RT.byteCast(arg)); }
    default int unary(int arg) { return (int) op(RT.byteCast(arg)); }
    default long unary(long arg) { return (long) op(RT.byteCast(arg)); }
    default float unary(float arg) { return (float) op(RT.byteCast(arg)); }
    default double unary(double arg) { return (double) op(RT.byteCast(arg)); }
    default Object unary(Object arg) { return op(RT.byteCast(arg)); }
  };
  public interface ShortUnary extends UnaryOperator
  {
    short op(short arg);
    default boolean unary(boolean arg) {
      return BooleanConversions.from(op(BooleanConversions.toShort(arg)));
    }
    default byte unary(byte arg) { return RT.byteCast(op(arg)); }
    default short unary(short arg) { return op(arg); }
    default char unary(char arg) { return (char) op(RT.shortCast(arg)); }
    default int unary(int arg) { return (int) op(RT.shortCast(arg)); }
    default long unary(long arg) { return (long) op(RT.shortCast(arg)); }
    default float unary(float arg) { return (float) op(RT.shortCast(arg)); }
    default double unary(double arg) { return (double) op(RT.shortCast(arg)); }
    default Object unary(Object arg) { return op(RT.shortCast(arg)); }

  };
  public interface IntUnary extends UnaryOperator
  {
    int op(int arg);
    default boolean unary(boolean arg) {
      return BooleanConversions.from(op(BooleanConversions.toInt(arg)));
    }
    default byte unary(byte arg) { return RT.byteCast(op(arg)); }
    default short unary(short arg) { return RT.shortCast(op(arg)); }
    default char unary(char arg) { return RT.charCast(op(arg)); }
    default int unary(int arg) { return op(arg); }
    default long unary(long arg) { return (long) op(RT.intCast(arg)); }
    default float unary(float arg) { return (float) op(RT.intCast(arg)); }
    default double unary(double arg) { return (double) op(RT.intCast(arg)); }
    default Object unary(Object arg) { return op(RT.intCast(arg)); }
  };
  public interface LongUnary extends UnaryOperator
  {
    long op(long arg);
    default boolean unary(boolean arg) {
      return BooleanConversions.from(op(BooleanConversions.toLong(arg)));
    }
    default byte unary(byte arg) { return RT.byteCast(op(arg)); }
    default short unary(short arg) { return RT.shortCast(op(arg)); }
    default char unary(char arg) { return RT.charCast(op(arg)); }
    default int unary(int arg) { return RT.intCast(op(arg)); }
    default long unary(long arg) { return op(arg); }
    default float unary(float arg) { return RT.floatCast(op(RT.longCast(arg))); }
    default double unary(double arg) { return (double) op(RT.longCast(arg)); }
    default Object unary(Object arg) { return op(RT.longCast(arg)); }
  };
  public interface FloatUnary extends UnaryOperator
  {
    float op(float arg);
    default boolean unary(boolean arg) {
      return BooleanConversions.from(op(BooleanConversions.toFloat(arg)));
    }
    default byte unary(byte arg) { return RT.byteCast(op(arg)); }
    default short unary(short arg) { return RT.shortCast(op(arg)); }
    default char unary(char arg) { return RT.charCast(op(arg)); }
    default int unary(int arg) { return RT.intCast(op(arg)); }
    default long unary(long arg) { return RT.uncheckedLongCast(op(RT.floatCast(arg))); }
    default float unary(float arg) { return op(arg); }
    default double unary(double arg) { return (double) op(RT.floatCast(arg)); }
    default Object unary(Object arg) { return op(RT.floatCast(arg)); }
  };
  public interface DoubleUnary extends UnaryOperator
  {
    double op(double arg);
    default boolean unary(boolean arg) {
      return BooleanConversions.from(op(BooleanConversions.toDouble(arg)));
    }
    default byte unary(byte arg) { return RT.byteCast(op(arg)); }
    default short unary(short arg) { return RT.shortCast(op(arg)); }
    default char unary(char arg) { return RT.charCast(op(arg)); }
    default int unary(int arg) { return RT.intCast(op(arg)); }
    default long unary(long arg) { return RT.longCast(op(RT.doubleCast(arg))); }
    default float unary(float arg) { return RT.floatCast(op(arg)); }
    default double unary(double arg) { return op(arg); }
    default Object unary(Object arg) { return op(RT.doubleCast(arg)); }
  };
  public interface ObjectUnary extends UnaryOperator
  {
    Object op(Object arg);
    default boolean unary(boolean arg) {
      return BooleanConversions.from(op(arg));
    }
    default byte unary(byte arg) { return RT.byteCast(op(arg)); }
    default short unary(short arg) { return RT.shortCast(op(arg)); }
    default char unary(char arg) { return RT.charCast(op(arg)); }
    default int unary(int arg) { return RT.intCast(op(arg)); }
    default long unary(long arg) { return RT.uncheckedLongCast(op(arg)); }
    default float unary(float arg) { return RT.floatCast(op(arg)); }
    default double unary(double arg) { return RT.doubleCast(op(arg)); }
    default Object unary(Object arg) { return op(arg); }
  };
}
