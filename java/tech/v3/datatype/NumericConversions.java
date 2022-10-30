package tech.v3.datatype;


import clojure.lang.RT;
import ham_fisted.Casts;

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
      return (int) ((char)arg);
    }
    else
      throw new RuntimeException("Invalid argument");
  }
  public static byte byteCast(Object arg) {
    return RT.byteCast(Casts.longCast(arg));
  }
  public static short shortCast(Object arg) {
    return RT.shortCast(Casts.longCast(arg));
  }
  public static char charCast(Object arg) {
    return Casts.charCast(arg);
  }
  public static int intCast(Object arg) {
    return RT.intCast(Casts.longCast(arg));
  }
  public static long longCast(Object arg) {
    return Casts.longCast(arg);
  }
  public static float floatCast(Object arg) {
    return RT.floatCast(Casts.doubleCast(arg));
  }
  public static double doubleCast(Object arg) {
    return RT.doubleCast(Casts.doubleCast(arg));
  }
}
