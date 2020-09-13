package tech.v3.datatype;

import clojure.lang.RT;

public class BinaryOperators
{
  public interface BooleanBinaryOperator extends BinaryOperator
  {
    default byte binaryByte(byte lhs, byte rhs) {
      return BooleanConversions.toByte(binaryBoolean( BooleanConversions.from(lhs),
						      BooleanConversions.from(rhs)));
    }
    default short binaryShort(short lhs, short rhs) {
      return BooleanConversions.toShort(binaryBoolean( BooleanConversions.from(lhs),
						      BooleanConversions.from(rhs)));
    }
    default char binaryChar(char lhs, char rhs) {
      return BooleanConversions.toChar(binaryBoolean( BooleanConversions.from(lhs),
						      BooleanConversions.from(rhs)));
    }
    default int binaryInt(int lhs, int rhs) {
      return BooleanConversions.toInt(binaryBoolean( BooleanConversions.from(lhs),
						      BooleanConversions.from(rhs)));
    }
    default long binaryLong(long lhs, long rhs) {
      return BooleanConversions.toLong(binaryBoolean( BooleanConversions.from(lhs),
						      BooleanConversions.from(rhs)));
    }
    default float binaryFloat(float lhs, float rhs) {
      return BooleanConversions.toFloat(binaryBoolean( BooleanConversions.from(lhs),
						      BooleanConversions.from(rhs)));
    }
    default double binaryDouble(double lhs, double rhs) {
      return BooleanConversions.toDouble(binaryBoolean( BooleanConversions.from(lhs),
						      BooleanConversions.from(rhs)));
    }
    default Object binaryObject(Object lhs, Object rhs) {
      return binaryBoolean(BooleanConversions.from(lhs),
			   BooleanConversions.from(rhs));
    }
  }
  public interface DoubleBinaryOperator extends BinaryOperator
  {
    default  boolean binaryBoolean(boolean lhs, boolean rhs) {
      return BooleanConversions.from(binaryDouble(BooleanConversions.toDouble(lhs),
						  BooleanConversions.toDouble(rhs)));
    }
    default byte binaryByte(byte lhs, byte rhs) {
      return RT.byteCast(binaryDouble((double) lhs, (double) rhs));
    }
    default short binaryShort(short lhs, short rhs) {
      return RT.shortCast(binaryDouble((double) lhs, (double) rhs));
    }
    default char binaryChar(char lhs, char rhs) {
      return RT.charCast(binaryDouble((double) lhs, (double) rhs));
    }
    default int binaryInt(int lhs, int rhs) {
      return RT.intCast(binaryDouble((double) lhs, (double) rhs));
    }
    default long binaryLong(long lhs, long rhs) {
      return RT.longCast(binaryDouble((double) lhs, (double) rhs));
    }
    default float binaryFloat(float lhs, float rhs) {
      return (float)binaryDouble((double) lhs, (double) rhs);
    }
    default Object binaryObject(Object lhs, Object rhs) {
      return binaryDouble(RT.doubleCast(lhs),RT.doubleCast(rhs));
    }
  }
  public interface LongBinaryOperator extends BinaryOperator
  {
    default  boolean binaryBoolean(boolean lhs, boolean rhs) {
      return BooleanConversions.from(binaryLong(BooleanConversions.toLong(lhs),
						BooleanConversions.toLong(rhs)));
    }
    default byte binaryByte(byte lhs, byte rhs) {
      return RT.byteCast(binaryLong((long) lhs, (long) rhs));
    }
    default short binaryShort(short lhs, short rhs) {
      return RT.shortCast(binaryLong((long) lhs, (long) rhs));
    }
    default char binaryChar(char lhs, char rhs) {
      return RT.charCast(binaryLong((long) lhs, (long) rhs));
    }
    default int binaryInt(int lhs, int rhs) {
      return RT.intCast(binaryLong((long) lhs, (long) rhs));
    }
    default float binaryFloat(float lhs, float rhs) {
      return RT.floatCast(binaryLong((long) lhs, (long) rhs));
    }
    default double binaryDouble(double lhs, double rhs) {
      return (double)binaryLong((long) lhs, (long) rhs);
    }
    default Object binaryObject(Object lhs, Object rhs) {
      return binaryLong(RT.longCast(lhs), RT.longCast(rhs));
    }
  }
  public interface ObjectBinaryOperator extends BinaryOperator
  {
    default  boolean binaryBoolean(boolean lhs, boolean rhs) {
      return BooleanConversions.from(binaryObject(lhs,rhs));
    }
    default byte binaryByte(byte lhs, byte rhs) {
      return RT.byteCast(binaryObject(lhs,rhs));
    }
    default short binaryShort(short lhs, short rhs) {
      return RT.shortCast(binaryObject(lhs,rhs));
    }
    default char binaryChar(char lhs, char rhs) {
      return RT.charCast(binaryObject(lhs,rhs));
    }
    default int binaryInt(int lhs, int rhs) {
      return RT.intCast(binaryObject(lhs,rhs));
    }
    default long binaryLong(long lhs, long rhs) {
      return RT.longCast(binaryObject(lhs,rhs));
    }
    default float binaryFloat(float lhs, float rhs) {
      return RT.floatCast(binaryObject(lhs,rhs));
    }
    default double binaryDouble(double lhs, double rhs) {
      return RT.doubleCast(binaryObject(lhs,rhs));
    }
  }
}
