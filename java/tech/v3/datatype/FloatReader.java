package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.stream.DoubleStream;


public interface FloatReader extends FloatIO
{
  default void write(long idx, float value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
