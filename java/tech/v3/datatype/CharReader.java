package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.stream.IntStream;


public interface CharReader extends PrimitiveReader
{
  char read(long idx);
  default void write(long idx, char value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
