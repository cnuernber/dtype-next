package tech.v3.datatype;

import clojure.lang.Symbol;
import clojure.lang.Compiler;
import clojure.lang.Namespace;
import clojure.lang.IFn;
import clojure.lang.Var;

public class ClojureHelper
{
  public static IFn findFn(String nsName, String varName) {
    return (IFn) ((Var) Compiler.resolveIn(Namespace.find(Symbol.intern(nsName)),
					   Symbol.intern(varName), false)).deref();
  }
}
