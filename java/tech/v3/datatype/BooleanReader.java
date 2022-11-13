package tech.v3.datatype;


import clojure.lang.Keyword;


public interface BooleanReader extends ObjectReader {
  default Object elemwiseDatatype () { return Keyword.intern(null, "boolean"); }
}
