package tech.v3.datatype;


import clojure.lang.RT;


public class BooleanConversions
{
  public static boolean from(boolean val) {
    return val;
  }
  public static boolean from(byte val) {
    return val != 0;
  }
  public static boolean from(short val) {
    return val != 0;
  }
  public static boolean from(char val) {
    return val != 0;
  }
  public static boolean from(int val) {
    return val != 0;
  }
  public static boolean from(long val) {
    return val != 0;
  }
  public static boolean from(float val) {
    return !Float.isNaN(val) && val != 0.0f;
  }
  public static boolean from(double val) {
    return !Double.isNaN(val) && val != 0.0;
  }
  public static boolean from(Object obj) {
    if (obj instanceof Number) {
      return from(RT.uncheckedDoubleCast(obj));
    } else if (obj instanceof Boolean) {
      return (boolean) obj;
    }
    else {
      return obj != null;
    }
  }

  public static byte toByte(boolean val) {
    return val ? (byte)1 : (byte)0;
  }
  public static short toShort(boolean val) {
    return val ? (short)1 : (short)0;
  }
  public static char toChar(boolean val) {
    return val ? (char)1 : (char)0;
  }
  public static int toInt(boolean val) {
    return val ? (int)1 : (int)0;
  }
  public static long toLong(boolean val) {
    return val ? (long)1 : (long)0;
  }
  public static float toFloat(boolean val) {
    return val ? (float)1 : (float)0;
  }
  public static double toDouble(boolean val) {
    return val ? (double)1 : (double)0;
  }
  public static Object toObject(boolean val) {
    return val;
  }
}
