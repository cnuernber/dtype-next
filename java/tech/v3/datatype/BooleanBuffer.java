package tech.v3.datatype;


import clojure.lang.Keyword;


public interface BooleanBuffer extends Buffer {
  default Object elemwiseDatatype () { return Keyword.intern(null, "boolean"); }
}
