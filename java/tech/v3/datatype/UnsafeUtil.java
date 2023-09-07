package tech.v3.datatype;


import java.nio.Buffer;
import java.nio.ByteBuffer;
import sun.misc.Unsafe;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import clojure.lang.RT;


public class UnsafeUtil {
  public static Unsafe getUnsafe() {
    try {
      Field f = Unsafe.class.getDeclaredField("theUnsafe");
      f.setAccessible(true);
      return Unsafe.class.cast(f.get(null));
    }
    catch(NoSuchFieldException e) {
      throw new IllegalStateException("sun.misc.Unsafe is not available in this JVM");
    }
    catch(IllegalAccessException e) {
      throw new IllegalStateException("sun.misc.Unsafe is not available in this JVM");
    }
  }

  public static Unsafe unsafe = getUnsafe();

  public static long copyBytes(final long src, final long dest, final long len) {
    if(len == 0) return dest;
    Unsafe us = getUnsafe();
    if(len < 128) {
      final int ilen = (int)len;
      for(int idx = 0; idx < ilen; ++idx)
	us.putByte(dest+idx, us.getByte(src+idx));
    } else {
      us.copyMemory(null, src, null, dest, len);
    }
    return dest;
  }

  public static byte[] copyBytesLoop(final long addr, byte[] dest, final int len) {
    Unsafe us = getUnsafe();
    for(int idx = 0; idx < len; ++idx)
      dest[idx] = us.getByte(addr+idx);
    return dest;
  }

  public static byte[] copyBytesMemcpy(final long addr, byte[] dest, final int len) {
    Unsafe us = getUnsafe();
    us.copyMemory(null,addr,dest, Unsafe.ARRAY_BYTE_BASE_OFFSET, len);
    return dest;
  }

  public static long copyBytes(byte[] src, final long dest, final int len) {
    if(len == 0) return dest;
    Unsafe us = getUnsafe();
    if(len < 128) {
      for(int idx = 0; idx < len; ++idx)
	us.putByte(dest+idx, src[idx]);
    } else {
      us.copyMemory(src, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, dest, len);
    }
    return dest;
  }

  public static String addrToString(final long addr, final int len) {
    Unsafe us = getUnsafe();
    if(addr == 0 || len == 0)
      return "";
    final byte[] buf = new byte[len];
    return new String(len < 128 ? copyBytesLoop(addr, buf, len) : copyBytesMemcpy(addr, buf, len));
  }

  public static long addressField() {
    try {
      return getUnsafe().objectFieldOffset(Buffer.class.getDeclaredField("address"));
    } catch (Throwable e) {
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
