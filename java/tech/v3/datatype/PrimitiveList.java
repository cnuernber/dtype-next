package tech.v3.datatype;


import java.util.Collection;
import java.util.RandomAccess;
import java.util.function.Consumer;
import clojure.lang.IReduceInit;


public interface PrimitiveList extends Buffer
{
  Object ensureCapacity(long cap);
}
