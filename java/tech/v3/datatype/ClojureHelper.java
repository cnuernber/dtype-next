package tech.v3.datatype;

import clojure.lang.Symbol;
import clojure.lang.Compiler;
import clojure.lang.Namespace;
import clojure.lang.IFn;
import clojure.lang.Var;
import clojure.lang.RT;

public class ClojureHelper
{
  static boolean initialized = false;
  public static boolean initialize() {
    //unsynchronized guard.  RT.init is synchronized so
    //this is faster but still safe.
    if (initialized == false) {
      initialized = true;
      RT.init();
      return true;
    }
    return false;
  }
  public static IFn findFn(String nsName, String varName) {
    try {
      //Without RT initialization .var can return nonsense.
      initialize();
      return RT.var(nsName, varName);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to find \"" + nsName + "/" + varName + "\"",
				 e);
    }
  }
}
