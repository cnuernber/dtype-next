package tech.v3.datatype;


import com.sun.jna.*;
import java.nio.ByteBuffer;


public class DirectMapped
{
  public static native Pointer memset(ByteBuffer p, int val, int len);
}
