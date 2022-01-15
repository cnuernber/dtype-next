package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.java.api.Clojure;
import clojure.lang.RT;
import java.util.Map;
import java.util.List;


public class Clj
{
  static final IFn symbolFn = Clojure.var("clojure.core", "symbol");
  static final IFn requireFn = Clojure.var("clojure.core", "require");
  static final IFn requireresFn = Clojure.var("clojure.core", "requiring-resolve");
  static final IFn keywordFn = Clojure.var("clojure.core", "keyword");
  static final IFn hashMapFn = Clojure.var("clojure.core", "hash-map");
  static final IFn vectorFn = Clojure.var("clojure.core", "vector");
  static final IFn vecFn = Clojure.var("clojure.core", "vec");
  static final IFn listFn = Clojure.var("clojure.core", "list");
  static final IFn rangeFn = Clojure.var("clojure.core", "range");
  static final IFn pushThreadBindingsFn = Clojure.var("clojure.core", "push-thread-bindings");
  static final IFn popThreadBindingsFn = Clojure.var("clojure.core", "pop-thread-bindings");

  static final IFn derefFn = Clojure.var("clojure.core", "deref");
  static final IFn atomFn = Clojure.var("clojure.core", "atom");
  static final IFn resetFn = Clojure.var("clojure.core", "reset!");
  static final IFn swapFn = Clojure.var("clojure.core", "swap!");
  static final IFn compareAndSetFn = Clojure.var("clojure.core", "compare-and-set!");


  public static Object symbol(String name) {
    return symbolFn.invoke(name);
  }
  public static Object symbol(String ns, String name) {
    return symbolFn.invoke(ns, name);
  }
  public static Object keyword(String name) {
    return keywordFn.invoke(name);
  }
  public static Object keyword(String ns, String name) {
    return symbolFn.invoke(ns, name);
  }
  public static Object require(String ns) {
    return requireFn.invoke(symbolFn.invoke(ns));
  }
  public static Object var(String ns, String name) {
    return Clojure.var(ns,name);
  }
  public static Object uncheckedRequiringResolve(String ns, String name) {
    return requireresFn.invoke(symbolFn.invoke(ns,name));
  }
  public static Object requiringResolve(String ns, String name) {
    Object retval = uncheckedRequiringResolve(ns, name);
    if (retval == null)
      throw new RuntimeException("Unable to resolve '" + ns + "/" + name);
    return retval;
  }

  public static Object call(Object obj) {
    return ((IFn) obj).invoke();
  }
  public static Object call(Object obj, Object arg1) {
    return ((IFn) obj).invoke(arg1);
  }
  public static Object call(Object obj, Object arg1, Object arg2) {
    return ((IFn) obj).invoke(arg1, arg2);
  }
  public static Object call(Object obj, Object arg1, Object arg2, Object arg3) {
    return ((IFn) obj).invoke(arg1, arg2, arg3);
  }
  public static Object call(Object obj, Object arg1, Object arg2, Object arg3, Object arg4) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4);
  }
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5);
  }
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6);
  }
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
  }
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7 ,arg8);
  }
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
  }
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9, Object arg10) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
  }
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9, Object arg10, Object arg11) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
			      arg11);
  }
  //You could go all the way up to 21 here.
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9, Object arg10, Object arg11, Object arg12) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
			      arg11, arg12);
  }
  public static Object apply(Object obj, Object... args) {
    return ((IFn) obj).applyTo(RT.seq(args));
  }
  public static Map persmap(Object... args) {
    return (Map)hashMapFn.applyTo(RT.seq(args));
  }
  public static List persvec(Object... args) {
    return (List)vectorFn.applyTo(RT.seq(args));
  }
  public static List vec(Object arglist) {
    return (List)vecFn.invoke(arglist);
  }
  public static Object range(Object end) {
    return rangeFn.invoke(end);
  }
  public static Object range(Object start, Object end) {
    return rangeFn.invoke(start, end);
  }
  public static Object range(Object start, Object end, Object increment) {
    return rangeFn.invoke(start, end, increment);
  }
  public static Object list(Object... args) {
    return listFn.applyTo(RT.seq(args));
  }
  public static void pushThreadBindings(Map varvalmap) {
    pushThreadBindingsFn.invoke(varvalmap);
  }

  public static void popThreadBindings() {
    popThreadBindingsFn.invoke();
  }

  public static AutoCloseable makeThreadBindings(Map varvalmap) {
    pushThreadBindings(varvalmap);
    return new AutoCloseable() {
      public void close() {
	popThreadBindings();
      }
    };
  }

  public static Object atom(Object val) {
    return atomFn.invoke(val);
  }
  public static Object reset(Object atom, Object val) {
    return resetFn.invoke(atom, val);
  }
  public static Object swap(Object atom, IFn swapFn) {
    return resetFn.invoke(atom, swapFn);
  }
  public static boolean compareAndSet(Object atom, Object oldval, Object newval) {
    return (boolean)compareAndSetFn.invoke(atom, oldval, newval);
  }
}
