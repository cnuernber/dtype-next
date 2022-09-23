package tech.v3.datatype;


import java.util.Collection;
import java.util.RandomAccess;
import java.util.function.Consumer;
import clojure.lang.IReduceInit;


public interface PrimitiveList extends Buffer
{
  void ensureCapacity(long cap);
  void addBoolean(boolean val);
  void addDouble(double val);
  void addLong(long val);
  void addObject(Object val);
  default boolean add(Object val) {
    addObject(val);
    return true;
  }
  default boolean addAll(Collection coll) {
    final int sz = size();
    if(coll != null) {
      if (coll instanceof RandomAccess) {
	ensureCapacity(coll.size() + sz);
      }
      //Prefer IReduce pathways over generic iteration
      if (coll instanceof IReduceInit) {
	((IReduceInit)coll).reduce(new IFnDef() {
	    public Object invoke(Object lhs, Object rhs) {
	      addObject(rhs);
	      return lhs;
	    }
	  }, this);
      }
      else {
	coll.forEach(new Consumer<Object>() {
	    public void accept(Object arg) {
	      addObject(arg);
	    }});
      }
    }
    return sz != size();
  }
}
