package tech.v3.datatype;

import clojure.lang.Counted;
import clojure.lang.RT;

public interface Countable extends Counted
{
  long lsize();
  default int count() { return RT.intCast(lsize()); }
};
