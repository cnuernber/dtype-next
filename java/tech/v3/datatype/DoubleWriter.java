package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface DoubleWriter extends PrimitiveWriter
{
  default double read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
};
