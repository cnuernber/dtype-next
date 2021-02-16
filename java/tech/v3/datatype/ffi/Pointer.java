package tech.v3.datatype.ffi;


public class Pointer
{
  public final long address;
  public Pointer(long addr) {
    address = addr;
  }
  public String toString() {
    return "{:address " + String.format("0x%016X", address) + " }";
  }
}
