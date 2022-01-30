package tech.v3.libs;

import static tech.v3.Clj.*;
import clojure.lang.IFn;


public class Nippy {
  private Nippy(){}

  // Load the type-specific bindings.
  static final Object nippyBindings = require("tech.v3.datatype.nippy");
  static final IFn freezeFn = requiringResolve("taoensso.nippy", "freeze");
  static final IFn thawFn = requiringResolve("taoensso.nippy", "thaw");


  public static final byte[] freeze(Object data) {
    return (byte[])freezeFn.invoke(data);
  }

  public static final Object thaw(byte[] data) {
    return thawFn.invoke(data);
  }
}
