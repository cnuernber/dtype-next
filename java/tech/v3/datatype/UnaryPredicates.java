package tech.v3.datatype;

import clojure.lang.RT;

public class UnaryPredicates
{
  public interface BooleanUnaryPredicate extends UnaryPredicate
  {
    default boolean unaryByte(byte arg) {
      return unaryBoolean( BooleanConversions.from(arg) );
    }
    default boolean unaryShort(short arg) {
      return unaryBoolean( BooleanConversions.from(arg) );
    }
    default boolean unaryChar(char arg) {
      return unaryBoolean( BooleanConversions.from(arg) );
    }
    default boolean unaryInt(int arg) {
      return unaryBoolean( BooleanConversions.from(arg) );
    }
    default boolean unaryLong(long arg) {
      return unaryBoolean( BooleanConversions.from(arg) );
    }
    default boolean unaryFloat(float arg) {
      return unaryBoolean( BooleanConversions.from(arg) );
    }
    default boolean unaryDouble(double arg) {
      return unaryBoolean( BooleanConversions.from(arg) );
    }
    default boolean unaryObject(Object arg) {
      return unaryBoolean(BooleanConversions.from(arg) );
    }
  }
  public interface DoubleUnaryPredicate extends UnaryPredicate
  {
    default  boolean unaryBoolean(boolean arg) {
      return unaryDouble(BooleanConversions.toDouble(arg));
    }
    default boolean unaryByte(byte arg) {
      return unaryDouble((double)arg);
    }
    default boolean unaryShort(short arg) {
      return unaryDouble((double)arg);
    }
    default boolean unaryChar(char arg) {
      return unaryDouble((double)arg);
    }
    default boolean unaryInt(int arg) {
      return unaryDouble((double)arg);
    }
    default boolean unaryLong(long arg) {
      return unaryDouble((double)arg);
    }
    default boolean unaryFloat(float arg) {
      return unaryDouble((double)arg);
    }
    default boolean unaryObject(Object arg) {
      return unaryDouble(RT.doubleCast(arg));
    }
  }
  public interface LongUnaryPredicate extends UnaryPredicate
  {
    default  boolean unaryBoolean(boolean arg) {
      return unaryLong(BooleanConversions.toLong(arg));
    }
    default boolean unaryByte(byte arg) {
      return unaryLong((long)arg);
    }
    default boolean unaryShort(short arg) {
      return unaryLong((long)arg);
    }
    default boolean unaryChar(char arg) {
      return unaryLong((long)arg);
    }
    default boolean unaryInt(int arg) {
      return unaryLong((long)arg);
    }
    default boolean unaryFloat(float arg) {
      return unaryLong((long)arg);
    }
    default boolean unaryDouble(double arg) {
      return unaryLong((long)arg);
    }
    default boolean unaryObject(Object arg) {
      return unaryLong(RT.longCast(arg));
    }
  }
  public interface ObjectUnaryPredicate extends UnaryPredicate
  {
    default  boolean unaryBoolean(boolean arg) {
      return unaryObject(arg);
    }
    default boolean unaryByte(byte arg) {
      return unaryObject(arg);
    }
    default boolean unaryShort(short arg) {
      return unaryObject(arg);
    }
    default boolean unaryChar(char arg) {
      return unaryObject(arg);
    }
    default boolean unaryInt(int arg) {
      return unaryObject(arg);
    }
    default boolean unaryLong(int arg) {
      return unaryObject(arg);
    }
    default boolean unaryFloat(float arg) {
      return unaryObject(arg);
    }
    default boolean unaryDouble(double arg) {
      return unaryObject(arg);
    }
  }
}
