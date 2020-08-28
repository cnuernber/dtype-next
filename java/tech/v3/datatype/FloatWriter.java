package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;


public interface FloatWriter extends PrimitiveIO
{
  default void write(long idx, float value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
