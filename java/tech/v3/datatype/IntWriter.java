package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface IntWriter extends IntIO
{
  default int read(long idx) { throw new UnsupportedOperationException(); }
  default boolean allowsRead() { return false; }
}
