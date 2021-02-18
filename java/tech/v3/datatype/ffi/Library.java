package tech.v3.datatype.ffi;


import clojure.lang.IDeref;


public interface Library extends IDeref {
  Pointer findSymbol(String symbolName);
}
