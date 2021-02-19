package tech.v3.datatype.ffi;


import clojure.lang.Murmur3;


public class Pointer
{
  public final long address;
  public Pointer(long addr) {
    address = addr;
  }
  public String toString() {
    return "{:address " + String.format("0x%016X", address) + " }";
  }
  public int hashCode() {
    return Murmur3.hashLong(address);
  }
  public boolean equals(Object other) {
    if(other instanceof Pointer) {
      return address == ((Pointer)other).address;
    } else {
      return false;
    }
  }
  public boolean isNil () {
    return address != 0;
  }
  public static Pointer constructNonZero(long val) {
    if (val != 0) {
      return new Pointer(val);
    }
    return null;
  }
}
