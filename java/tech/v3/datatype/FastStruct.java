package tech.v3.datatype;


import clojure.lang.PersistentArrayMap;
import clojure.lang.APersistentMap;
import clojure.lang.IPersistentMap;
import clojure.lang.IPersistentCollection;
import clojure.lang.IObj;
import clojure.lang.Obj;
import clojure.lang.ISeq;
import clojure.lang.MapEntry;
import clojure.lang.IMapEntry;
import clojure.lang.ASeq;
import clojure.lang.Util;
import clojure.lang.RT;
import clojure.lang.IFn;
import clojure.lang.IReduceInit;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.RandomAccess;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Function;
import ham_fisted.IFnDef;
import ham_fisted.Reductions;
import ham_fisted.Casts;
import ham_fisted.IMutList;



public class FastStruct extends APersistentMap implements IObj, IReduceInit {
  public final Map slots; //HashMaps are faster than persistent maps.
  public final List vals;
  public final IPersistentMap ext;
  public final IPersistentMap meta;
  public final int sz;

  public FastStruct(IPersistentMap _meta, Map _slots,
		    List _vals, IPersistentMap _ext) {
    this.meta = _meta;
    this.ext = _ext;
    this.slots = _slots;
    this.vals = _vals;
    sz = slots.size() + ((ext == null) ? 0 : ext.count());
  }

  public FastStruct(Map _slots, List _vals) {
    this( PersistentArrayMap.EMPTY, _slots, _vals,
	  PersistentArrayMap.EMPTY);
  }

  public int columnIndex(Object key) throws Exception {
    Object v = slots.get(key);
    if (v != null) {
      return RT.uncheckedIntCast(v);
    }
    throw new Exception("Key was not found");
  }

  public IObj withMeta(IPersistentMap _meta){
    if(meta == _meta)
      return this;
    return new FastStruct (_meta, slots, vals, ext);
  }

  public IPersistentMap meta(){
    return meta;
  }

  public boolean containsKey(Object key){
    return slots.containsKey(key) || ext.containsKey(key);
  }

  public IMapEntry entryAt(Object key){
    Object v = slots.get(key);
    if(v != null)
      return MapEntry.create(key, vals.get(RT.uncheckedIntCast(v)));
    return ext.entryAt(key);
  }

  public IPersistentMap assoc(Object key, Object val){
    Object v = slots.get(key);
    if(v != null) {
      int i = RT.uncheckedIntCast( v );
      List newVals = new ArrayList(vals);
      newVals.set(i,val);
      return new FastStruct(meta, slots, Collections.unmodifiableList(newVals), ext);
    }
    return new FastStruct(meta, slots, vals, ext.assoc(key, val));
  }

  public Object valAt(Object key){
    Object i = slots.get(key);
    if(i != null)
      return vals.get((int)(Casts.longCast(i)));

    return ext.valAt(key);
  }

  public Object valAt(Object key, Object notFound){
    Object i = slots.get(key);
    if(i != null) {
      return vals.get(RT.uncheckedIntCast(i));
    }
    return ext.valAt(key, notFound);
  }

  public IPersistentMap assocEx(Object key, Object val) {
    if(containsKey(key))
      throw Util.runtimeException("Key already present");
    return assoc(key, val);
  }

  public IPersistentMap without(Object key) {
    if(slots.containsKey(key)) {
      LinkedHashMap newSlots = new LinkedHashMap(slots);
      newSlots.remove(key);
      return new FastStruct(meta, Collections.unmodifiableMap(newSlots), vals, ext);
    }
    IPersistentMap newExt = ext.without(key);
    if(newExt == ext)
      return this;
    return new FastStruct(meta, slots, vals, newExt);
  }

  public Iterator iterator(){
    return new Iterator(){
      private Iterator ks = slots.entrySet().iterator();
      private Iterator extIter = ext == null ? null : ext.iterator();
      public boolean hasNext(){
	if (ks != null) {
	  if (ks.hasNext())
	    return true;
	  ks = null;
	}
	if (extIter != null) {
	  if (extIter.hasNext()) {
	    return true;
	  }
	  extIter = null;
	}
	return false;
      }

      public Object next(){
	if(ks != null) {
	  Map.Entry data = (Map.Entry) ks.next();
	  int valIdx = RT.uncheckedIntCast(data.getValue());
	  return new MapEntry( data.getKey(), vals.get(valIdx));
	}
	else if (extIter != null) {
	  return extIter.next();
	}
	else
	  throw new NoSuchElementException();
      }
      public void remove(){
	throw new UnsupportedOperationException();
      }
    };
  }

  public int size() { return sz; }

  public int count() { return sz; }

  public boolean equals(Object other) { return equiv(other); }
  public boolean equiv(Object other) {
    if(this == other) return true;
    if(other == null) return false;
    if(!(other instanceof Map)) return false;
    final Map om = (Map) other;
    if (om.size() != size()) return false;

    //Fastpath of we are the same map with only potentially
    //different values.
    if(other instanceof FastStruct &&
       slots == ((FastStruct) other).slots) {
      return Util.equiv(vals, ((FastStruct)other).vals) &&
	Util.equiv(ext, ((FastStruct)other).ext);
    }
    for(Object obj: om.entrySet()) {
      final Map.Entry me = (Map.Entry)obj;
      final Map.Entry mm = entryAt(me.getKey());
      if(!Util.equiv(me.getValue(), mm.getValue()))
	return false;
    }
    return true;
  }

  public ISeq seq(){
    return RT.chunkIteratorSeq(iterator());
  }

  public IPersistentCollection empty(){
    ArrayList newData = new ArrayList(slots.size());
    for (int idx = 0; idx < slots.size(); ++idx)
      newData.add(null);
    return new FastStruct(slots, newData);
  }

  public static class FMapEntry implements IMutList, Map.Entry {
    public final Object k;
    public final Object v;
    int _hash;
    public FMapEntry(Object _k, Object _v) {
      k = _k;
      v = _v;
      _hash = 0;
    }
    public boolean equals(Object o) { return equiv(o); }
    public int hashCode() { return hasheq(); }
    public int hasheq() {
      if (_hash == 0)
	_hash = IMutList.super.hasheq();
      return _hash;
    }
    public Object setValue( Object v) { throw new RuntimeException("Cannot set value."); }
    public Object getKey() { return k; }
    public Object getValue() { return v; }
    public int size() { return 2; }
    public Object get(int idx) {
      if(idx == 0) return k;
      if(idx == 1) return v;
      throw new RuntimeException("Index out of range: " + String.valueOf(idx));
    }
  }

  public Object reduce(IFn rfn, Object init) {
    Iterator iter = slots.entrySet().iterator();
    while(iter.hasNext() && !RT.isReduced(init)) {
      final Map.Entry me = (Map.Entry)iter.next();
      final long idx = Casts.longCast(me.getValue());
      init = rfn.invoke(init, new FMapEntry(me.getKey(), vals.get((int)idx)));
    }
    if( ext != null && !RT.isReduced(init))
      return Reductions.serialReduction(rfn, init, ext);
    else
      return Reductions.unreduce(init);
  }

  /**
   * Create a factory that will create map implementations based on a single list of values.
   * Values have to be in the same order as column names.
   */
  public static IFn createFactory(List colnames) {
    int nEntries = colnames.size();
    if( nEntries == 0 ) {
      throw new RuntimeException("No column names provided");
    }
    LinkedHashMap slots = new LinkedHashMap(nEntries);
    for (int idx = 0; idx < nEntries; ++idx ) {
      slots.put(colnames.get(idx), idx);
    }
    Map constSlots = (Map)Collections.unmodifiableMap(slots);
    if( colnames.size() != slots.size() ) {
      throw new RuntimeException("Duplicate colname name: " + String.valueOf(slots));
    }
    return new IFnDef.OO() {
      public Object invoke(Object values) {
	if(!(values instanceof RandomAccess)) throw new RuntimeException("Values must be a random access list.");
	final List valList = (List)values;
	if( slots.size() != valList.size() ) {
	  throw new RuntimeException("Number of values: " + String.valueOf(valList.size()) +
				     " doesn't equal the number of keys: " + String.valueOf(slots.size()));
	}
	return new FastStruct(constSlots, valList);
      }
    };
  }

  public static FastStruct createFromColumnNames(List colnames, List vals) {
    return (FastStruct)createFactory(colnames).invoke(vals);
  }
}
