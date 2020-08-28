package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.stream.IntStream;


public interface IntReader extends IntIO
{
  default void write(long idx, byte value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
