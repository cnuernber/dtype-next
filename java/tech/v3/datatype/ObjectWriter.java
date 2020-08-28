package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface ObjectWriter extends PrimitiveWriter
{
  default Object read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
