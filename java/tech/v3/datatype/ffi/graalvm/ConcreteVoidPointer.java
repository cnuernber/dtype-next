package tech.v3.datatype.ffi.graalvm;

import org.graalvm.word.ComparableWord;
import org.graalvm.nativeimage.c.type.VoidPointer;


public class ConcreteVoidPointer
  extends tech.v3.datatype.ffi.Pointer
  implements VoidPointer
{
  public ConcreteVoidPointer(long addr) { super(addr); }
  public boolean isNull() { return address == 0; }
  public boolean isNonNull() { return address != 0; }
  public long rawValue() { return address; }
  public boolean notEqual(ComparableWord other)
  {
    return address != other.rawValue();
  }
  public boolean equal(ComparableWord other)
  {
    return address == other.rawValue();
  }
}
