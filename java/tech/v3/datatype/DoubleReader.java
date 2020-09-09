package tech.v3.datatype;

import clojure.lang.Keyword;
import java.util.stream.DoubleStream;


public interface DoubleReader extends DoubleIO
{
  default void writeDouble(long idx, double value) { throw new UnsupportedOperationException(); }
  default boolean supportsWrite() { return false; }
}
