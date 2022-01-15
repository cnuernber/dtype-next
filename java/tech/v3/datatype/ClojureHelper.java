package tech.v3.datatype;

import clojure.java.api.Clojure;
import clojure.lang.IFn;

public class ClojureHelper
{
  public static IFn findFn(String nsName, String varName) {
    return (IFn)Clojure.var(nsName, varName);
  }
}
