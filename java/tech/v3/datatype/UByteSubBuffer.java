package tech.v3.datatype;


import ham_fisted.Casts;
import ham_fisted.ArrayLists;
import ham_fisted.ArraySection;
import ham_fisted.ChunkedList;
import clojure.lang.IPersistentMap;
import clojure.lang.IFn;
import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.Arrays;

public class UByteSubBuffer implements LongBuffer, ArrayLists.ArrayOwner {
  final byte[] data;
  final int sidx;
  final int eidx;
  final int nElems;
  final IPersistentMap meta;
  public UByteSubBuffer(byte[] d, int si, int ei, IPersistentMap m) {
    this.data = d;
    sidx = si;
    eidx = ei;
    nElems = ei - si;
    meta = m;
  }
  public Keyword elemwiseDatatype() { return Keyword.intern(null, "uint8"); }
  public Buffer cloneList() { return new UByteSubBuffer((byte[])copyOf(nElems), 0, nElems, meta); }
  public IPersistentMap meta() { return meta; }
  public Buffer withMeta(IPersistentMap m) {
    return new UByteSubBuffer(data, sidx, eidx, m);
  }
  public boolean allowsRead() { return true; }
  public boolean allowsWrite() { return true; }
  public long lsize() { return nElems; }
  public boolean equals(Object o) { return equiv(o); }
  public int hashCode() { return hasheq(); }
  public Buffer subBuffer(long ssidx, long seidx) {
    ChunkedList.sublistCheck(ssidx, seidx, nElems);
    return new UByteSubBuffer(data, sidx + (int)ssidx, sidx + (int)seidx, meta);
  }
  public ArraySection getArraySection() { return new ArraySection(data, sidx, eidx); }
  public long readLong(long idx) {
    return Byte.toUnsignedInt(data[ChunkedList.indexCheck(sidx, nElems, (int)idx)]);
  }

  public void writeLong(long idx, long val) {
    data[ChunkedList.indexCheck(sidx, nElems, (int)idx)] = NumericConversions.ubyteHostCast(val);
  }
  public Object longReduction(IFn.OLO rfn, Object init) {
    final int ne = (int)nElems;
    for(int idx = 0; idx < ne && !RT.isReduced(init); ++idx) {
      init = rfn.invokePrim(init, Byte.toUnsignedInt(data[idx+sidx]));
    }
    return init;
  }
  public void move(int sidx, int eidx, int count) {
    ChunkedList.checkIndexRange(0, size(), eidx, eidx + count);
    System.arraycopy(data, sidx, data, eidx, count);
  }
  public Object copyOf(int len) {
    return Arrays.copyOfRange(data, sidx, sidx+len);
  }
  public Object copyOfRange(int ssidx, int seidx) {
    return Arrays.copyOfRange(data, sidx+ssidx, sidx+seidx);
  }
  public void fill(int ssidx, int seidx, Object v) {
    final byte val = NumericConversions.ubyteHostCast(Casts.longCast(v));
    Arrays.fill(data, sidx+ssidx, sidx+seidx, val);
  }
}
