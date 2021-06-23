package tech.v3.datatype;


import clojure.lang.APersistentVector;
import clojure.lang.IPersistentVector;
import clojure.lang.IPersistentCollection;
import clojure.lang.IPersistentStack;
import clojure.lang.Indexed;
import clojure.lang.RT;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;


public class ListPersistentVector extends APersistentVector
{
  public final List data;
  public ListPersistentVector (List _data) {
    data = _data;
  }
  public IPersistentVector cons(Object val) {
    ArrayList newData = new ArrayList(data);
    newData.add(val);
    return new ListPersistentVector(Collections.unmodifiableList(newData));
  }
  public IPersistentVector assocN(int idx, Object val) {
    ArrayList newData = new ArrayList(data);
    newData.set(idx,val);
    return new ListPersistentVector(Collections.unmodifiableList(newData));
  }
  public IPersistentCollection empty() {
    return new ListPersistentVector(Collections.unmodifiableList(new ArrayList()));
  }
  public IPersistentStack pop() {
    if (data.size() != 0)
      return new ListPersistentVector(data.subList(0, data.size()-1));
    return this;
  }
  public int count() {
    return data.size();
  }
  public Object nth(int idx) {
    return data.get(idx);
  }
  public Object invoke(Object arg) {
    long var = RT.uncheckedIntCast(arg);
    var = var < 0 ? size() + var : var;
    return nth((int)var);
  }
}
