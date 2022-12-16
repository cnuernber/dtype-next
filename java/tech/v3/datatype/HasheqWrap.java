package tech.v3.datatype;

import clojure.lang.IHashEq;
import clojure.lang.Util;
import clojure.lang.IDeref;
import java.util.Objects;


// Clojure objects often cache their hasheq value but *not* their hashcode...
public class HasheqWrap implements IDeref {
  public final Object data;

  public HasheqWrap(Object d) {
    data = Objects.requireNonNull( d, "Null wrap not implemented.");
  }
  public final int hashCode() { return ((IHashEq)data).hasheq(); }
  public final boolean equals(Object o) {
    return Util.equiv(data, (o instanceof HasheqWrap) ? ((HasheqWrap)o).data : o);
  }
  public final String toString() { return data.toString(); }
  public Object deref() { return data; }
}
