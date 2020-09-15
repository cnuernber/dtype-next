package tech.v3.datatype;


import clojure.lang.RT;

//Helpers for converting into numbers
public class NumericConversions
{
  public static Number numberCast(Object arg) {
    if (arg instanceof Number) {
      return (Number)arg;
    } else if (arg instanceof Boolean) {
      if ((Boolean)arg)
	return 1;
      else
	return 0;
    }
    else if (arg instanceof Character) {
      return Character.getNumericValue((Character)arg);
    }
    else
      throw new RuntimeException("Invalid argument");
  }
  public static byte byteCast(Object arg) {
    return RT.byteCast(numberCast(arg));
  }
  public static short shortCast(Object arg) {
    return RT.shortCast(numberCast(arg));
  }
  public static char charCast(Object arg) {
    return RT.charCast(arg);
  }
  public static int intCast(Object arg) {
    return RT.intCast(numberCast(arg));
  }
  public static long longCast(Object arg) {
    return RT.longCast(numberCast(arg));
  }
  public static float floatCast(Object arg) {
    return RT.floatCast(numberCast(arg));
  }
  public static double doubleCast(Object arg) {
    return RT.doubleCast(numberCast(arg));
  }
}
