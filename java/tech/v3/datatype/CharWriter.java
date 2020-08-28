package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface CharWriter extends CharIO
{
  default char read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
