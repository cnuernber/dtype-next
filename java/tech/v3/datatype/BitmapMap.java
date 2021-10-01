package tech.v3.datatype;


import clojure.lang.PersistentStructMap;
import clojure.lang.PersistentHashMap;
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
import com.github.ztellman.primitive_math.Primitives;
import java.io.Serializable;
import java.util.NoSuchElementException;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Collections;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.IntIterator;



public class BitmapMap extends APersistentMap implements IObj{
  public final RoaringBitmap slots;
  public final Object value;
  public final IPersistentMap ext;
  public final IPersistentMap meta;

  public BitmapMap(IPersistentMap _meta, RoaringBitmap _slots,
		   Object _value, IPersistentMap _ext) {
    this.meta = _meta;
    this.ext = _ext;
    this.slots = _slots;
    this.value = _value;
  }

  public BitmapMap(RoaringBitmap _slots, Object _value) {
    this( PersistentHashMap.EMPTY, _slots, _value,
	  PersistentHashMap.EMPTY);
  }

  public IObj withMeta(IPersistentMap _meta){
    if(meta == _meta)
      return this;
    return new BitmapMap (_meta, slots, value, ext);
  }

  public IPersistentMap meta(){
    return meta;
  }

  public boolean containsKey(Object key){
    return (key instanceof Number && slots.contains(RT.intCast(key)))
      || ext.containsKey(key);
  }

  public IMapEntry entryAt(Object key){
    if(key instanceof Number && slots.contains(RT.intCast(key))) {
      return MapEntry.create(key, value);
    }
    return ext.entryAt(key);
  }

  public IPersistentMap assoc(Object key, Object val){
    if(key instanceof Number && slots.contains(RT.intCast(key))) {
      throw new UnsupportedOperationException();
    }
    return new BitmapMap(meta, slots, value, ext.assoc(key, val));
  }

  public IPersistentMap assocEx(Object key, Object val) {
    if(containsKey(key))
      throw Util.runtimeException("Key already present");
    return assoc(key, val);
  }

  public Object valAt(Object key){
    if(key instanceof Number && slots.contains(RT.intCast(key))) {
      return value;
    }
    return ext.valAt(key);
  }

  public Object valAt(Object key, Object notFound){
    if(key instanceof Number && slots.contains(RT.intCast(key))) {
      return value;
    }
    return ext.valAt(key, notFound);
  }

  public IPersistentMap without(Object key) {
    if(key instanceof Number && slots.contains(RT.intCast(key))) {
      RoaringBitmap newSlots = slots.clone();
      newSlots.remove(RT.intCast(key));
      return new BitmapMap(meta, newSlots, value, ext);
    }
    IPersistentMap newExt = ext.without(key);
    if(newExt == ext)
      return this;
    return new BitmapMap(meta, slots, value, newExt);
  }

  public Iterator iterator(){
    return new Iterator(){
      private IntIterator ks = slots.getIntIterator();
      private Iterator extIter = ext == null ? null : ext.iterator();
      public boolean hasNext(){
	return (ks != null && ks.hasNext() || (extIter != null && extIter.hasNext()));
      }

      public Object next(){
	if(ks != null) {
	  //These will be unsigned ints...
	  long idx = (long)ks.next();
	  ks = ks.hasNext() ? ks : null;
	  return new MapEntry( idx, value );
	}
	else if(extIter != null && extIter.hasNext()) {
	  Object data = extIter.next();
	  extIter = extIter.hasNext() ? extIter : null;
	  return data;
	}
	else
	  throw new NoSuchElementException();
      }
      public void remove(){
	throw new UnsupportedOperationException();
      }
    };
  }

  public int count(){
    return slots.getCardinality() + RT.count(ext);
  }

  public ISeq seq(){
    return RT.chunkIteratorSeq(iterator());
  }

  public IPersistentCollection empty(){
    return new BitmapMap(slots, null);
  }
}
