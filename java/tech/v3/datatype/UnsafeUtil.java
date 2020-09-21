package tech.v3.datatype;


import java.nio.Buffer;
import sun.misc.Unsafe;


public class UnsafeUtil {
  public static long addressField() {
    try {
      return xerial.larray.buffer.UnsafeUtil.getUnsafe().objectFieldOffset(Buffer.class.getDeclaredField("address"));
    }catch (Throwable e) {
      return -1;
    }
  }
  public static final long addressFieldOffset = addressField();
}
