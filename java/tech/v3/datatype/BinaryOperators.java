package tech.v3.datatype;

import clojure.lang.RT;

public class BinaryOperators {
  public interface BooleanBinary extends BinaryOperator
  {
    boolean op(boolean lhs, boolean rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return op(lhs,rhs);
    }
    default byte binary(byte lhs, byte rhs) {
      return BooleanConversions.toByte(op(BooleanConversions.from(lhs),
					  BooleanConversions.from(rhs)));
    }
    default short binary(short lhs, short rhs) {
      return BooleanConversions.toShort(op(BooleanConversions.from(lhs),
					   BooleanConversions.from(rhs)));
    }
    default char binary(char lhs, char rhs) {
      return BooleanConversions.toChar(op(BooleanConversions.from(lhs),
					  BooleanConversions.from(rhs)));
    }
    default int binary(int lhs, int rhs) {
      return BooleanConversions.toInt(op(BooleanConversions.from(lhs),
					 BooleanConversions.from(rhs)));
    }
    default long binary(long lhs, long rhs) {
      return BooleanConversions.toLong(op(BooleanConversions.from(lhs),
					  BooleanConversions.from(rhs)));
    }
    default float binary(float lhs, float rhs) {
      return BooleanConversions.toFloat(op(BooleanConversions.from(lhs),
					   BooleanConversions.from(rhs)));
    }
    default double binary(double lhs, double rhs) {
      return BooleanConversions.toDouble(op(BooleanConversions.from(lhs),
					    BooleanConversions.from(rhs)));
    }
    default Object binary(Object lhs, Object rhs) {
      return op(BooleanConversions.from(lhs),
		BooleanConversions.from(rhs));
    }
  };
  public interface ByteBinary extends BinaryOperator
  {
    byte op(byte lhs, byte rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(BooleanConversions.toByte(lhs),
					BooleanConversions.toByte(rhs)));
    }
    default byte binary(byte lhs, byte rhs) { return op(lhs, rhs); }
    default short binary(short lhs, short rhs) { return (short) op(RT.byteCast(lhs), RT.byteCast(rhs)); }
    default char binary(char lhs, char rhs) { return (char) op(RT.byteCast(lhs), RT.byteCast(rhs)); }
    default int binary(int lhs, int rhs) { return (int) op(RT.byteCast(lhs), RT.byteCast(rhs)); }
    default long binary(long lhs, long rhs) { return (long) op(RT.byteCast(lhs), RT.byteCast(rhs)); }
    default float binary(float lhs, float rhs) { return (float) op(RT.byteCast(lhs), RT.byteCast(rhs)); }
    default double binary(double lhs, double rhs) { return (double) op(RT.byteCast(lhs), RT.byteCast(rhs)); }
    default Object binary(Object lhs, Object rhs) { return op(RT.byteCast(lhs), RT.byteCast(rhs)); }
  };

  public interface ShortBinary extends BinaryOperator
  {
    short op(short lhs, short rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(BooleanConversions.toShort(lhs),
					BooleanConversions.toShort(rhs)));
    }
    default byte binary(byte lhs, byte rhs) { return RT.byteCast(op(lhs, rhs)); }
    default short binary(short lhs, short rhs) { return op(lhs, rhs); }
    default char binary(char lhs, char rhs) { return RT.charCast(op(RT.shortCast(lhs), RT.shortCast(rhs))); }
    default int binary(int lhs, int rhs) { return (int) op(RT.shortCast(lhs), RT.shortCast(rhs)); }
    default long binary(long lhs, long rhs) { return (long) op(RT.shortCast(lhs), RT.shortCast(rhs)); }
    default float binary(float lhs, float rhs) { return (float) op(RT.shortCast(lhs), RT.shortCast(rhs)); }
    default double binary(double lhs, double rhs) { return (double) op(RT.shortCast(lhs), RT.shortCast(rhs)); }
    default Object binary(Object lhs, Object rhs) { return op(RT.shortCast(lhs), RT.shortCast(rhs)); }
  };
  public interface CharBinary extends BinaryOperator
  {
    char op(char lhs, char rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(BooleanConversions.toChar(lhs),
					BooleanConversions.toChar(rhs)));
    }
    default byte binary(byte lhs, byte rhs) { return RT.byteCast(op((char)lhs, (char)rhs)); }
    default short binary(short lhs, short rhs) { return RT.shortCast(op(RT.charCast(lhs), RT.charCast(rhs))); }
    default char binary(char lhs, char rhs) { return op(lhs, rhs); }
    default int binary(int lhs, int rhs) { return (int) op(RT.charCast(lhs), RT.charCast(rhs)); }
    default long binary(long lhs, long rhs) { return (long) op(RT.charCast(lhs), RT.charCast(rhs)); }
    default float binary(float lhs, float rhs) { return (float) op(RT.charCast(lhs), RT.charCast(rhs)); }
    default double binary(double lhs, double rhs) { return (double) op(RT.charCast(lhs), RT.charCast(rhs)); }
    default Object binary(Object lhs, Object rhs) { return op(RT.charCast(lhs), RT.charCast(rhs)); }
  };
  public interface IntBinary extends BinaryOperator
  {
    int op(int lhs, int rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(BooleanConversions.toInt(lhs),
					BooleanConversions.toInt(rhs)));
    }
    default byte binary(byte lhs, byte rhs) { return RT.byteCast(op(lhs, rhs)); }
    default short binary(short lhs, short rhs) { return RT.shortCast(op(lhs, rhs)); }
    default char binary(char lhs, char rhs) { return RT.charCast(op(lhs, rhs)); }
    default int binary(int lhs, int rhs) { return op(lhs, rhs); }
    default long binary(long lhs, long rhs) { return (long) op(RT.intCast(lhs), RT.intCast(rhs)); }
    default float binary(float lhs, float rhs) { return (float) op(RT.intCast(lhs), RT.intCast(rhs)); }
    default double binary(double lhs, double rhs) { return (double) op(RT.intCast(lhs), RT.intCast(rhs)); }
    default Object binary(Object lhs, Object rhs) { return op(RT.intCast(lhs), RT.intCast(rhs)); }
  };
  public interface LongBinary extends BinaryOperator
  {
    long op(long lhs, long rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(BooleanConversions.toLong(lhs),
					BooleanConversions.toLong(rhs)));
    }
    default byte binary(byte lhs, byte rhs) { return RT.byteCast(op(lhs, rhs)); }
    default short binary(short lhs, short rhs) { return RT.shortCast(op(lhs, rhs)); }
    default char binary(char lhs, char rhs) { return RT.charCast(op(lhs, rhs)); }
    default int binary(int lhs, int rhs) { return RT.intCast(op(lhs, rhs)); }
    default long binary(long lhs, long rhs) { return op(lhs, rhs); }
    default float binary(float lhs, float rhs) { return (float) op(RT.longCast(lhs), RT.longCast(rhs)); }
    default double binary(double lhs, double rhs) { return (double) op(RT.longCast(lhs), RT.longCast(rhs)); }
    default Object binary(Object lhs, Object rhs) { return op(RT.longCast(lhs), RT.longCast(rhs)); }
  };
  public interface FloatBinary extends BinaryOperator
  {
    float op(float lhs, float rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(BooleanConversions.toFloat(lhs),
					BooleanConversions.toFloat(rhs)));
    }
    default byte binary(byte lhs, byte rhs) { return RT.byteCast(op(lhs, rhs)); }
    default short binary(short lhs, short rhs) { return RT.shortCast(op(lhs, rhs)); }
    default char binary(char lhs, char rhs) { return RT.charCast(op(lhs, rhs)); }
    default int binary(int lhs, int rhs) { return RT.intCast(op(lhs, rhs)); }
    default long binary(long lhs, long rhs) { return RT.longCast(op(RT.floatCast(lhs), RT.floatCast(rhs))); }
    default float binary(float lhs, float rhs) { return op(lhs, rhs); }
    default double binary(double lhs, double rhs) { return (double) op(RT.floatCast(lhs), RT.floatCast(rhs)); }
    default Object binary(Object lhs, Object rhs) { return op(RT.floatCast(lhs), RT.floatCast(rhs)); }
  };
  public interface DoubleBinary extends BinaryOperator
  {
    double op(double lhs, double rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(BooleanConversions.toDouble(lhs),
					BooleanConversions.toDouble(rhs)));
    }
    default byte binary(byte lhs, byte rhs) { return RT.byteCast(op(lhs, rhs)); }
    default short binary(short lhs, short rhs) { return RT.shortCast(op(lhs, rhs)); }
    default char binary(char lhs, char rhs) { return RT.charCast(op(lhs, rhs)); }
    default int binary(int lhs, int rhs) { return RT.intCast(op(lhs, rhs)); }
    default long binary(long lhs, long rhs) { return RT.longCast(op(RT.doubleCast(lhs), RT.doubleCast(rhs))); }
    default float binary(float lhs, float rhs) { return RT.floatCast(op(RT.doubleCast(lhs), RT.doubleCast(rhs))); }
    default double binary(double lhs, double rhs) { return op(lhs, rhs); }
    default Object binary(Object lhs, Object rhs) { return op(RT.doubleCast(lhs), RT.doubleCast(rhs)); }
  };
  public interface ObjectBinary extends BinaryOperator
  {
    Object op(Object lhs, Object rhs);
    default boolean binary(boolean lhs, boolean rhs) {
      return BooleanConversions.from(op(lhs,rhs));
    }
    default byte binary(byte lhs, byte rhs) { return RT.byteCast(op(lhs, rhs)); }
    default short binary(short lhs, short rhs) { return RT.shortCast(op(lhs, rhs)); }
    default char binary(char lhs, char rhs) { return RT.charCast(op(lhs, rhs)); }
    default int binary(int lhs, int rhs) { return RT.intCast(op(lhs, rhs)); }
    default long binary(long lhs, long rhs) { return RT.longCast(op(lhs,rhs)); }
    default float binary(float lhs, float rhs) { return RT.floatCast(op(lhs,rhs)); }
    default double binary(double lhs, double rhs) { return RT.doubleCast(op(lhs,rhs)); }
    default Object binary(Object lhs, Object rhs) { return op(lhs, rhs); }
  };



}
