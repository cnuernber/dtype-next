package tech.v3.datatype;

import clojure.lang.RT;

public class BinaryPredicates
{
  public interface BooleanBinaryPredicate extends BinaryPredicate
  {
    default boolean binaryByte(byte lhs, byte rhs) {
      return binaryBoolean( BooleanConversions.from(lhs),
			    BooleanConversions.from(rhs) );
    }
    default boolean binaryShort(short lhs, short rhs) {
      return binaryBoolean( BooleanConversions.from(lhs),
			    BooleanConversions.from(rhs) );
    }
    default boolean binaryChar(char lhs, char rhs) {
      return binaryBoolean( BooleanConversions.from(lhs),
			    BooleanConversions.from(rhs) );
    }
    default boolean binaryInt(int lhs, int rhs) {
      return binaryBoolean( BooleanConversions.from(lhs),
			    BooleanConversions.from(rhs) );
    }
    default boolean binaryLong(long lhs, long rhs) {
      return binaryBoolean( BooleanConversions.from(lhs),
			    BooleanConversions.from(rhs) );
    }
    default boolean binaryFloat(float lhs, float rhs) {
      return binaryBoolean( BooleanConversions.from(lhs),
			    BooleanConversions.from(rhs) );
    }
    default boolean binaryDouble(double lhs, double rhs) {
      return binaryBoolean( BooleanConversions.from(lhs),
			    BooleanConversions.from(rhs) );
    }
    default boolean binaryObject(Object lhs, Object rhs) {
      return binaryBoolean(BooleanConversions.from(lhs),
			   BooleanConversions.from(rhs) );
    }
  }
  public interface DoubleBinaryPredicate extends BinaryPredicate
  {
    default  boolean binaryBoolean(boolean lhs, boolean rhs) {
      return binaryDouble(BooleanConversions.toDouble(lhs),
			  BooleanConversions.toDouble(rhs));
    }
    default boolean binaryByte(byte lhs, byte rhs) {
      return binaryDouble((double)lhs,(double)rhs);
    }
    default boolean binaryShort(short lhs, short rhs) {
      return binaryDouble((double)lhs,(double)rhs);
    }
    default boolean binaryChar(char lhs, char rhs) {
      return binaryDouble((double)lhs,(double)rhs);
    }
    default boolean binaryInt(int lhs, int rhs) {
      return binaryDouble((double)lhs,(double)rhs);
    }
    default boolean binaryLong(long lhs, long rhs) {
      return binaryDouble((double)lhs,(double)rhs);
    }
    default boolean binaryFloat(float lhs, float rhs) {
      return binaryDouble((double)lhs,(double)rhs);
    }
    default boolean binaryObject(Object lhs, Object rhs) {
      return binaryDouble(RT.doubleCast(lhs),RT.doubleCast(lhs));
    }
  }
  public interface LongBinaryPredicate extends BinaryPredicate
  {
    default  boolean binaryBoolean(boolean lhs, boolean rhs) {
      return binaryLong(BooleanConversions.toLong(lhs),
			BooleanConversions.toLong(rhs));
    }
    default boolean binaryByte(byte lhs, byte rhs) {
      return binaryLong((long)lhs,(long)rhs);
    }
    default boolean binaryShort(short lhs, short rhs) {
      return binaryLong((long)lhs,(long)rhs);
    }
    default boolean binaryChar(char lhs, char rhs) {
      return binaryLong((long)lhs,(long)rhs);
    }
    default boolean binaryInt(int lhs, int rhs) {
      return binaryLong((long)lhs,(long)rhs);
    }
    default boolean binaryFloat(float lhs, float rhs) {
      return binaryLong((long)lhs,(long)rhs);
    }
    default boolean binaryDouble(double lhs, double rhs) {
      return binaryLong((long)lhs,(long)rhs);
    }
    default boolean binaryObject(Object lhs, Object rhs) {
      return binaryLong(RT.longCast(lhs),RT.longCast(rhs));
    }
  }
  public interface ObjectBinaryPredicate extends BinaryPredicate
  {
    default  boolean binaryBoolean(boolean lhs, boolean rhs) {
      return binaryObject(lhs,rhs);
    }
    default boolean binaryByte(byte lhs, byte rhs) {
      return binaryObject(lhs,rhs);
    }
    default boolean binaryShort(short lhs, short rhs) {
      return binaryObject(lhs,rhs);
    }
    default boolean binaryChar(char lhs, char rhs) {
      return binaryObject(lhs,rhs);
    }
    default boolean binaryInt(int lhs, int rhs) {
      return binaryObject(lhs,rhs);
    }
    default boolean binaryLong(long lhs, long rhs) {
      return binaryObject(lhs,rhs);
    }
    default boolean binaryFloat(float lhs, float rhs) {
      return binaryObject(lhs,rhs);
    }
    default boolean binaryDouble(double lhs, double rhs) {
      return binaryObject(lhs,rhs);
    }
  }
}
