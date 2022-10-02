package tech.v3.datatype;


import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.DoubleConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.List;
import ham_fisted.IMutList;
import ham_fisted.ArrayLists;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import clojure.lang.IFn;
import clojure.lang.IDeref;
import clojure.lang.Keyword;


public class MutListBuffer implements Buffer {
  public final IMutList data;
  public final boolean supportsWrite;
  public final Keyword elemwiseDatatype;

  public MutListBuffer(IMutList _data, boolean _supportsWrite, Keyword ewiseDt) {
    data = _data;
    supportsWrite = _supportsWrite;
    elemwiseDatatype = ewiseDt;
  }
  public boolean allowsRead() { return true; }
  public boolean allowsWrite() { return supportsWrite; }
  public Object elemwiseDatatype () { return elemwiseDatatype; }
  public List subList(int sidx, int eidx) {
    return new MutListBuffer((IMutList)data.subList(sidx, eidx), supportsWrite, elemwiseDatatype);
  }
  public IMutList cloneList() { return new MutListBuffer((IMutList)data.cloneList(), supportsWrite, elemwiseDatatype); }
  public long lsize() { return data.size(); }
  public IPersistentMap meta() { return data.meta(); }
  public IObj withMeta(IPersistentMap m) {
    return new MutListBuffer((IMutList)data.withMeta(m), supportsWrite, elemwiseDatatype); }
  public String toString() { return data.toString(); }
  public boolean readBoolean(long idx) { return data.getBoolean((int)idx); }
  public byte readByte(long idx) { return (byte)data.getLong((int)idx); }
  public short readShort(long idx) { return (short)data.getLong((int)idx); }
  public char readChar(long idx) { return (char)data.getLong((int)idx); }
  public int readInt(long idx) { return (int)data.getLong((int)idx); }
  public long readLong(long idx) { return data.getLong((int)idx); }
  public float readFloat(long idx) { return (float)data.getDouble((int)idx); }
  public double readDouble(long idx) { return (double)data.getDouble((int)idx); }
  public Object readObject(long idx) { return data.get((int)idx); }
  public void writeBoolean(long idx, boolean val) { data.setBoolean((int)idx, val); }
  public void writeByte(long idx, byte val) { data.setLong((int)idx, val); }
  public void writeShort(long idx, short val) { data.setLong((int)idx, val); }
  public void writeChar(long idx, char val) { data.setLong((int)idx, (long)val); }
  public void writeInt(long idx, int val) { data.setLong((int)idx, val); }
  public void writeLong(long idx, long val) { data.setLong((int)idx, val); }
  public void writeFloat(long idx, float val) { data.setDouble((int)idx, val); }
  public void writeDouble(long idx, double val) { data.setDouble((int)idx, val); }
  public void writeObject(long idx, Object val) { data.set((int)idx, val); }
  public Object get(int idx) { return data.get(idx); }
  public Object set(int idx, Object val) { return data.set(idx, val); }
  // public void accPlusLong(int idx, long val) {
  //   data.accPlusLong(idx, val);
  // }
  // public void accPlusDouble(int idx, double val) {
  //   data.accPlusDouble(idx, val);
  // }
  // public void accumPlusLong(long idx, long val) {
  //   data.accPlusLong((int)idx, val);
  // }
  // public void accumPlusDouble(long idx, double val) {
  //   data.accPlusDouble((int)idx, val);
  // }
  public Object reduce(IFn f) {
    return data.reduce(f);
  }
  public Object reduce(IFn f, Object init) {
    return data.reduce(f, init);
  }
  public Object kvreduce(IFn f, Object init) {
    return data.kvreduce(f, init);
  }
  public double doubleReduction(DoubleBinaryOperator op, double init) {
    return data.doubleReduction(op, init);
  }
  public long longReduction(LongBinaryOperator op, long init) {
    return data.longReduction(op, init);
  }
  public void doubleForEach(DoubleConsumer c) {
    data.doubleForEach(c);
  }
  public void longForEach(LongConsumer c) {
    data.longForEach(c);
  }
  public void genericForEach(Consumer c) {
    data.genericForEach(c);
  }
}
