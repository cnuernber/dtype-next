package tech.v3.datatype;


import clojure.lang.IFn;
import clojure.lang.IObj;
import clojure.lang.Keyword;
import clojure.lang.IPersistentMap;
import clojure.lang.RT;
import ham_fisted.Transformables;
import ham_fisted.CljHash;
import ham_fisted.Reductions;
import ham_fisted.IFnDef;
import ham_fisted.IMutList;
import ham_fisted.Casts;
import java.util.Collection;


public class PackingMutListBuffer implements Buffer {
  public final IMutList data;
  public final boolean supportsWrite;
  public final Keyword elemwiseDatatype;
  public final IFn packFn; //obj->long
  public final IFn unpackFn; //long->obj
  int _hash;
  public PackingMutListBuffer(IMutList _data, boolean _supportsWrite, Keyword ewiseDt,
			      IFn packFn, IFn unpackFn) {
    data = _data;
    supportsWrite = _supportsWrite;
    elemwiseDatatype = ewiseDt;
    this.packFn = packFn;
    this.unpackFn = unpackFn;
  }
  public PackingMutListBuffer(PackingMutListBuffer other, Object data) {
    this.data = (IMutList)data;
    supportsWrite = other.supportsWrite;
    elemwiseDatatype = other.elemwiseDatatype;
    packFn = other.packFn;
    unpackFn = other.unpackFn;
  }
  public boolean allowsRead() { return true; }
  public boolean allowsWrite() { return supportsWrite; }
  public Object elemwiseDatatype () { return elemwiseDatatype; }
  public long lsize() { return data.size(); }
  public int size() { return data.size(); }
  public IMutList subList(int sidx, int eidx) {
    return new PackingMutListBuffer(this, (IMutList)data.subList(sidx, eidx));
  }
  public Buffer subBuffer(long sidx, long eidx) {
    return (Buffer)subList(RT.intCast(sidx), RT.intCast(eidx));
  }
  public IMutList cloneList() { return new PackingMutListBuffer(this, data.cloneList()); }
  public IObj withMeta(IPersistentMap m) {
    return new PackingMutListBuffer(this, data.withMeta(m));
  }
  public int hashCode() { return hasheq(); }
  public boolean equals(Object other) { return equiv(other); }
  public int hasheq() {
    if (_hash == 0 )
      _hash = CljHash.listHasheq(this);
    return _hash;
  }
  public boolean equiv(Object other) {
    return CljHash.listEquiv(this, other);
  }
  public String toString() { return Transformables.sequenceToString(this); }
  public long readLong(long idx) { return data.getLong((int)idx); }
  public void writeLong(long idx, long val) { data.setLong((int)idx, val); }
  public Object readObject(long idx) { return unpackFn.invoke(data.getLong((int)idx)); }
  public void writeObject(long idx, Object obj) {
    data.setLong((int)idx, Casts.longCast(packFn.invoke(obj)));
  }
  public boolean add(Object v) { return data.add(packFn.invoke(v)); }
  public void addLong(long v) { data.addLong(v); }
  public boolean addAll(Collection c) {
    return addAllReducible(c);
  }
}
