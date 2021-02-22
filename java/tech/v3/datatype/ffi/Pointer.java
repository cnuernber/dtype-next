package tech.v3.datatype.ffi;


import clojure.lang.Murmur3;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;


public class Pointer implements IObj
{
  public final long address;
  public final IPersistentMap metadata;
  public Pointer(long addr, IPersistentMap _metadata) {
    address = addr;
    metadata = _metadata;
  }
  public Pointer(long addr) {
    this(addr, null);
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
    return address == 0;
  }
  public static Pointer constructNonZero(long val) {
    if (val != 0) {
      return new Pointer(val);
    }
    return null;
  }
  public IPersistentMap meta() { return metadata; }
  public IObj withMeta(IPersistentMap metadata) {
    return new Pointer(address, metadata);
  }
}
