package tech.v3.datatype;

import clojure.lang.Keyword;
import clojure.lang.RT;
import java.util.stream.Stream;


public interface ObjectReader extends PrimitiveReader
{
  default void write(long idx, Object value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
