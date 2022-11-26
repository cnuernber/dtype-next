package tech.v3.datatype;


import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.DoubleConsumer;
import java.util.function.LongBinaryOperator;
import java.util.function.DoubleBinaryOperator;
import java.util.stream.Stream;
import java.util.stream.LongStream;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.List;
import java.util.Comparator;
import java.util.Collection;
import java.util.Random;
import ham_fisted.IMutList;
import ham_fisted.ArrayLists;
import ham_fisted.Transformables;
import ham_fisted.ParallelOptions;
import clojure.lang.IObj;
import clojure.lang.IPersistentMap;
import clojure.lang.IFn;
import clojure.lang.IDeref;
import clojure.lang.Keyword;
import clojure.lang.RT;
import it.unimi.dsi.fastutil.objects.ObjectArrays;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.floats.FloatComparator;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;


public class MutListBuffer implements Buffer, Cloneable {
  public final IMutList data;
  public final boolean supportsWrite;
  public final Keyword elemwiseDatatype;

  public MutListBuffer(IMutList _data, boolean _supportsWrite, Keyword ewiseDt) {
    data = _data;
    supportsWrite = _supportsWrite;
    elemwiseDatatype = ewiseDt;
  }
  MutListBuffer(MutListBuffer other, Object _data) {
    data = (IMutList)_data;
    supportsWrite = other.supportsWrite;
    elemwiseDatatype = other.elemwiseDatatype;
  }
  public boolean allowsRead() { return true; }
  public boolean allowsWrite() { return supportsWrite; }
  public Object elemwiseDatatype () { return elemwiseDatatype; }
  public IMutList subList(int sidx, int eidx) {
    return new MutListBuffer(this, (IMutList)data.subList(sidx, eidx));
  }
  public Buffer subBuffer(long sidx, long eidx) {
    return (Buffer)subList(RT.intCast(sidx), RT.intCast(eidx));
  }
  public IMutList cloneList() { return new MutListBuffer(this, data.cloneList()); }
  public Object clone() { return cloneList(); }
  public long lsize() { return data.size(); }
  public int size() { return data.size(); }
  public IPersistentMap meta() { return data.meta(); }
  public IObj withMeta(IPersistentMap m) {
    return new MutListBuffer(this, data.withMeta(m));
  }
  public int hashCode() { return data.hashCode(); }
  public boolean equals(Object other) { return data.equals(other); }
  public int hasheq() { return data.hasheq(); }
  public boolean equiv(Object other) { return data.equiv(other); }
  public String toString() { return data.toString(); }
  public long readLong(long idx) { return data.getLong((int)idx); }
  public double readDouble(long idx) { return (double)data.getDouble((int)idx); }
  public Object readObject(long idx) { return data.get((int)idx); }
  public void writeLong(long idx, long val) { data.setLong((int)idx, val); }
  public void writeDouble(long idx, double val) { data.setDouble((int)idx, val); }
  public void writeObject(long idx, Object val) { data.set((int)idx, val); }
  public Object get(int idx) { return data.get(idx); }
  public Object set(int idx, Object val) { return data.set(idx, val); }
  public void setLong(int idx, long v) { data.setLong(idx, v);; }
  public void setDouble(int idx, double v) { data.setDouble(idx, v); }
  public long getLong(int idx) { return data.getLong(idx); }
  public double getDouble(int idx) { return data.getDouble(idx); }
  public boolean add(Object v) { return data.add(v); }
  public void add(int idx, Object v) { data.add(idx, v); }
  public void addLong(long v) { data.addLong(v); }
  public void addDouble(double v) { data.addDouble(v); }
  public boolean addAll(Collection c) {
    return data.addAll(c);
  }
  @SuppressWarnings("unchecked")
  public boolean addAllReducible(Object obj) {
    return data.addAllReducible(obj);
  }
  public void removeRange(int startidx, int endidx) {
    data.removeRange(startidx, endidx);
  }
  @SuppressWarnings("unchecked")
  public void fillRange(int startidx, final int endidx, Object v) {
    data.fillRange(startidx, endidx, v);
  }
  @SuppressWarnings("unchecked")
  public void fillRange(int startidx, List v) {
    data.fillRange(startidx, v);
  }
  public Object toNativeArray() { return data.toNativeArray(); }
  public int[] toIntArray() { return data.toIntArray(); }
  public long[] toLongArray() { return data.toLongArray(); }
  public float[] toFloatArray() { return data.toFloatArray(); }
  public double[] toDoubleArray() { return data.toDoubleArray(); }
  public IntComparator indexComparator() { return data.indexComparator(); }
  public IntComparator indexComparator(Comparator c) { return data.indexComparator(c); }
  public int[] sortIndirect(Comparator c) { return data.sortIndirect(c); }
  public void sort(Comparator c) { data.sort(c); }
  public List immutSort(Comparator c) { return new MutListBuffer( this, data.immutSort(c) ); }
  public void shuffle(Random r) { data.shuffle(r); }
  public List immutShuffle(Random r) { return new MutListBuffer( this, data.immutShuffle(r) ); }
  public List reindex(int[] indexes) { return new MutListBuffer( this, data.reindex(indexes) ); }
  public int binarySearch(Object v, Comparator c) { return data.binarySearch(v, c); }
  public List reverse() { return new MutListBuffer(this, data.reverse()); }
  public void accPlusLong(int idx, long val) {
    data.accPlusLong(idx, val);
  }
  public void accPlusDouble(int idx, double val) {
    data.accPlusDouble(idx, val);
  }
  public void accumPlusLong(long idx, long val) {
    data.accPlusLong((int)idx, val);
  }
  public void accumPlusDouble(long idx, double val) {
    data.accPlusDouble((int)idx, val);
  }
  public Object reduce(IFn f) {
    return data.reduce(f);
  }
  public Object reduce(IFn f, Object init) {
    return data.reduce(f, init);
  }
  public Object kvreduce(IFn f, Object init) {
    return data.kvreduce(f, init);
  }
  public Object parallelReduction(IFn initValFn, IFn rfn, IFn mergeFn,
				  ParallelOptions options) {
    return data.parallelReduction(initValFn, rfn, mergeFn, options);
  }
  public void forEach(Consumer c) {
    data.forEach(c);
  }

  public LongStream indexStream(boolean parallel) {
    return data.indexStream(parallel);
  }

  public Stream objStream(boolean parallel) {
    return data.objStream(parallel);
  }

  public DoubleStream doubleStream(boolean parallel) {
    return data.doubleStream(parallel);
  }

  public LongStream longStream(boolean parallel) {
    return data.longStream(parallel);
  }

  public IntStream intStream(boolean parallel) {
    return indexStream(parallel).mapToInt((long idx)->RT.intCast(getLong((int)idx)));
  }
}
