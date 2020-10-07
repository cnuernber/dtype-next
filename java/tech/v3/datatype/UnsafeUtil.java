package tech.v3.datatype;


import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;
import java.lang.reflect.Constructor;
import clojure.lang.RT;


public class UnsafeUtil {
  public static long addressField() {
    try {
      return xerial.larray.buffer.UnsafeUtil.getUnsafe().objectFieldOffset(Buffer.class.getDeclaredField("address"));
    }catch (Throwable e) {
      return -1;
    }
  }
  public static Constructor<?> findDirectBufferConstructor() {
    try {
      ByteBuffer direct = ByteBuffer.allocateDirect(1);
      final Constructor<?> constructor =
	direct.getClass().getDeclaredConstructor(long.class, int.class);
      constructor.setAccessible(true);
      return constructor;
    } catch (Throwable e) {
      return null;
    }
  }
  public static final long addressFieldOffset = addressField();

  public static final Constructor<?> directBufferConstructor = findDirectBufferConstructor();
  public static Object constructByteBufferFromAddress(long address, long nBytes) throws Exception {
    return  directBufferConstructor.newInstance(address, RT.intCast(nBytes));
  }
}
