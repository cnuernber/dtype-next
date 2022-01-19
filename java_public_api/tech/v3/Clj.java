package tech.v3;

import clojure.lang.IFn;
import clojure.java.api.Clojure;
import clojure.lang.RT;
import clojure.lang.IPersistentMap;
import clojure.lang.Symbol;
import clojure.lang.Keyword;
import java.util.Map;
import java.util.List;


/**
 * Static methods to make using Clojure much easier via Java.  The least verbose
 * way of using this class is to use it as a static import.  One thing to note
 * is that we provide a convenience interface, tech.v3.datatype.IFnDef that provides
 * default implementations for all of the many IFn invoke arities so you can very
 * easily create an implementation of clojure.lang.IFn like so:
 *
 * <pre>
 * return new tech.v3.datatype.IFnDef() {
 *   public Object invoke(Object lhs, Object rhs) {
 *     return doYourThing(lhs,rhs);
 *   }
 * };
 * </pre>
 */
public class Clj
{
  //No need to construct anything.
  private Clj() {}

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
  static final IFn applyFn = Clojure.var("clojure.core", "apply");
  static final IFn metaFn = Clojure.var("clojure.core", "meta");
  static final IFn withMetaFn = Clojure.var("clojure.core", "with-meta");
  static final IFn varyMetaFn = Clojure.var ("clojure.core", "vary-meta");
  static final IFn keysFn = Clojure.var("clojure.core", "keys");
  static final IFn valsFn = Clojure.var("clojure.core", "vals");
  static final IFn compileFn = Clojure.var("clojure.core", "compile");
  static final Object compilePathVar = Clojure.var("clojure.core", "*compile-path*");

  /**
   * merge fn.  Useful to pass into update or varyMeta.
   */
  public static final IFn mergeFn = Clojure.var("clojure.core", "merge");
  /**
   * dissoc fn.  Useful to pass into update or varyMeta.
   */
  public static final IFn assocFn = Clojure.var("clojure.core", "assoc");
  /**
   * dissoc fn.  Useful to pass into update or varyMeta.
   */
  public static final IFn dissocFn = Clojure.var("clojure.core", "dissoc");
  /**
   * update fn.  Useful to pass into varyMeta.
   */
  public static final IFn updateFn = Clojure.var("clojure.core", "update");


  /**
   * Create a Clojure symbol object from a string.
   */
  public static Symbol symbol(String name) {
    return (Symbol)symbolFn.invoke(name);
  }
  /**
   * Create a Clojure symbol object from a namespace name and a string.
   */
  public static Symbol symbol(String ns, String name) {
    return (Symbol)symbolFn.invoke(ns, name);
  }
  /**
   * Create a Clojure keyword from a string.  Keywords are used extremely frequently
   * in Clojure so there is a shorthand method - kw.
   * @see kw.
   */
  public static Keyword keyword(String name) {
    return (Keyword)keywordFn.invoke(name);
  }
  /**
   * Create a Clojure keyword from a string.
   */
  public static Keyword kw(String name) {
    return (Keyword)keywordFn.invoke(name);
  }
  /**
   * Create a Clojure namespaced keyword from a namespace name and a string.
   */
  public static Keyword keyword(String ns, String name) {
    return (Keyword)keywordFn.invoke(ns, name);
  }
  /**
   * Ask the clojure runtime to require a particular namespace.  This must be used
   * before a 'var' lookup but is not required to be used before a 'requiringResolve'
   * lookup.
   */
  public static Object require(String ns) {
    return requireFn.invoke(symbolFn.invoke(ns));
  }
  /**
   * Find a Clojure public var from a previously required namespace.
   *
   * This method returns null on if the var cannot be found.
   *
   * @see require.
   */
  public static IFn var(String ns, String name) {
    return Clojure.var(ns,name);
  }

  /**
   * Perform a require and then lookup a var.  Returns 'null' on failure.
   */
  public static IFn uncheckedRequiringResolve(String ns, String name) {
    return (IFn)requireresFn.invoke(symbolFn.invoke(ns,name));
  }
  /**
   * Perform a require and then lookup a var.  Throws exception if the var
   * isn't found.  If an exception isn't desired, use 'uncheckedRequiringResolve'.
   */
  public static IFn requiringResolve(String ns, String name) {
    IFn retval = uncheckedRequiringResolve(ns, name);
    if (retval == null)
      throw new RuntimeException("Unable to resolve '" + ns + "/" + name + "'");
    return retval;
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj) {
    return ((IFn) obj).invoke();
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj, Object arg1) {
    return ((IFn) obj).invoke(arg1);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj, Object arg1, Object arg2) {
    return ((IFn) obj).invoke(arg1, arg2);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj, Object arg1, Object arg2, Object arg3) {
    return ((IFn) obj).invoke(arg1, arg2, arg3);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj, Object arg1, Object arg2, Object arg3, Object arg4) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7 ,arg8);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9, Object arg10) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9, Object arg10, Object arg11) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
			      arg11);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn.
   */
  //You could go all the way up to 21 here.
  public static Object call(Object obj,
			    Object arg1, Object arg2, Object arg3, Object arg4,
			    Object arg5, Object arg6, Object arg7, Object arg8,
			    Object arg9, Object arg10, Object arg11, Object arg12) {
    return ((IFn) obj).invoke(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
			      arg11, arg12);
  }
  /**
   * Invoke an implementation of clojure.lang.IFn with variable arguments.
   */
  public static Object apply(Object obj, Object... args) {
    return ((IFn) obj).applyTo(RT.seq(args));
  }
  /**
   * Create a Clojure persistent map with the clojure.core.hash-map function.
   */
  public static Map hashmap(Object... args) {
    return (Map)hashMapFn.applyTo(RT.seq(args));
  }
  /**
   * Create a Clojure persistent vector with the clojure.core.vector function.
   */
  public static List vector(Object... args) {
    return (List)vectorFn.applyTo(RT.seq(args));
  }
  /**
   * Create a Clojure persistent vector with the clojure.core.vec function - this
   * version takes a single argument that can be an iterable or an array or a derivative
   * of java.util.List.
   */
  public static List vec(Object arglist) {
    return (List)vecFn.invoke(arglist);
  }
  /**
   * Merge a left hashmap with a right hashmap, the rightmost hashmap wins on key conflict.
   */
  public static Object merge(Object leftMap, Object rightMap) {
    return call(mergeFn, leftMap, rightMap);
  }
  /**
   * Merge a left hashmap with more hashmaps, the rightmost hashmap wins on key conflict.
   */
  public static Object merge(Object leftMap, Object rightMap, Object...maps) {
    return call(applyFn, mergeFn, leftMap, rightMap, maps);
  }
  /**
   * Assoc a new key,val into the map.  Returns a new map.
   */
  public static Object assoc(Object mapOrNil, Object key, Object val) {
    return call(assocFn, mapOrNil, key, val);
  }
  /**
   * Assoc new key,vals into the map.  Returns a new map.
   */
  public static Object assoc(Object mapOrNil, Object key, Object val, Object...args) {
    return call(applyFn, assocFn, mapOrNil, key, val, args);
  }
  /**
   * Dissoc a key from a map returning a new map.
   */
  public static Object dissoc(Object mapOrNil, Object key) {
    return call(dissocFn, mapOrNil, key);
  }
  /**
   * Dissoc keys from a map returning a new map.
   */
  public static Object dissoc(Object mapOrNil, Object key, Object... keys) {
    return call(applyFn, dissocFn, mapOrNil, key, keys);
  }
  /**
   * Update a value at a specific key by passing in a function that gets the
   * previous value and must return a new one.  Key does not need to previously
   * exist.  Returns a new map.
   */
  public static Object update(Object mapOrNil, Object key, IFn updateFn) {
    return call(updateFn, mapOrNil, key, updateFn);
  }

  /**
   * Create a Clojure range from a single numeric endpoint.
   */
  public static Object range(Object end) {
    return rangeFn.invoke(end);
  }
  /**
   * Create a Clojure range from a start and end.  Range will be
   * [start, end).
   */
  public static Object range(Object start, Object end) {
    return rangeFn.invoke(start, end);
  }
  /**
   * Create a Clojure range from a start, end, and increment.  Range will be
   * [start, end).
   */
  public static Object range(Object start, Object end, Object increment) {
    return rangeFn.invoke(start, end, increment);
  }
  /**
   * Create a Clojure persistent list.
   */
  public static Object list(Object... args) {
    return listFn.applyTo(RT.seq(args));
  }
  /**
   * !!USE WITH CARE!!! - push new thread bindings.  This is best left ignored
   * unless you need it to interact with Clojure libraries.  popThreadBindings *must*
   * be called within the same thread e.g. in a finally clause.
   * varvalmap is a map of Clojure Var (the result of 'var' or 'requiringResolve')
   * to values.  The vars in the map must be dynamic vars else the per-thread bindings
   * not not work.
   */
  public static void pushThreadBindings(Map varvalmap) {
    pushThreadBindingsFn.invoke(varvalmap);
  }
  /**
   * !!USE WITH CARE!! - pop thread bindings that were previously pushed.
   */
  public static void popThreadBindings() {
    popThreadBindingsFn.invoke();
  }
  /**
   * !!USE WITH CARE!! - pushThreadBindings and then return an auto-closeable thread bindings
   * object that will pop the thread bindings on close.  Meant to be used within a
   * try-with-resources pattern.
   */
  public static AutoCloseable makeThreadBindings(Map varvalmap) {
    pushThreadBindings(varvalmap);
    return new AutoCloseable() {
      public void close() {
	popThreadBindings();
      }
    };
  }
  /**
   * Deref a Clojure deref'able object.  These include at least vars, atoms, futures, many
   * custom object types.  These types all derive from clojure.lang.IDeref at the very
   * least.
   */
  public static Object deref(Object data) {
    return derefFn.invoke(data);
  }
  /**
   * Create a Clojure atom.  Atoms allow simple, efficient, and safe multi-threaded use cases.
   * To get the value of the atom call 'deref'.
   *
   * @see deref.
   */
  public static Object atom(Object val) {
    return atomFn.invoke(val);
  }
  /**
   * Reset a Clojure atom to a particular value indepedent of its previous contents.
   * @see atom.
   */
  public static Object reset(Object atom, Object val) {
    return resetFn.invoke(atom, val);
  }
  /**
   * Swap a Clojure atom using a function that receives its previous value and must
   * return a new value.  See namespace comments for easy ways to create an implementation
   * of clojure.lang.IFn.
   * @see atom.
   */
  public static Object swap(Object atom, IFn userSwapFn) {
    return swapFn.invoke(atom, userSwapFn);
  }
  /**
   * Swap a Clojure atom using a function that receives its previous value followed by
   * args and and must return a new value.  See namespace comments for easy ways to create
   * an implementation of clojure.lang.IFn.
   * @see atom.
   */
  public static Object swap(Object atom, IFn userSwapFn, Object... args) {
    return call(applyFn, swapFn, atom, userSwapFn, args);
  }

  /**
   * Conditionally set the value of the atom to newval if the current contents match
   * exactly oldval returning true on success.
   * @see atom.
   */
  public static boolean compareAndSet(Object atom, Object oldval, Object newval) {
    return (boolean)compareAndSetFn.invoke(atom, oldval, newval);
  }

  /**
   * Return the metadata stored on an object.  Most Clojure objects implement IMeta and this
   * is a wrapper around calling its meta member fn.
   */
  public static Map meta(Object val) {
    return (Map)metaFn.invoke(val);
  }
  /**
   * Store different metadata on an Object.  Most Clojure objects implement IObj and this
   * is a wrapper around calling its withMeta member fn.
   */
  public static Object withMeta(Object val, Object data) {
    return withMetaFn.invoke(val, (IPersistentMap)data);
  }

  /**
   * Change the metadata by passing in a function that receives the old value and must return
   * either null or a new PersistentMap value.
   */
  public static Object varyMeta(Object val, IFn modifyFn) {
    return varyMetaFn.invoke(val, modifyFn);
  }
  /**
   * Change the metadata by passing in a function that recieves the old value as the
   * first argument and then any additional args.  Function must return either null or
   * a new PersistentMap.
   */
  public static Object varyMeta(Object val, IFn modifyFn, Object... args) {
    return applyFn.invoke(varyMetaFn, val, modifyFn, args);
  }

  /**
   * Return the keys of a map.
   */
  public static Iterable keys(Object val) {
    return (Iterable)keysFn.invoke(val);
  }
  /**
   * Return the values of a map.
   */
  public static Iterable vals(Object val) {
    return (Iterable)valsFn.invoke(val);
  }

  /**
   * <p>Compile a clojure namespace into class files.  Compilation path defaults to
   * 'classes'.  If this compilation pathway is on the classpath then that namespace
   * will load potentially much faster next time it is 'require'd.</p>
   *
   * <p>The major caveat here is if you upgrade the base clojure library you must recompile.
   * <b>A difference between the version of the .clj files and the version of the
   * .class files will lead to unpredictable errors running the code.</b></p>
   */
  public static void compile(String namespace) {
    compileFn.invoke(symbol(namespace));
  }

  /**
   * <p>Compile a clojure namespace into class files located in a specific output directory.
   * If this output directoryis on the classpath then that namespace will load potentially
   * much faster next time it is 'require'd.</p>
   *
   * <p>outputDir must exist.</p>
   *
   * <p>The major caveat here is if you upgrade the base clojure library you must recompile.
   * <b>A difference between the version of the .clj files and the version of the
   * .class files will lead to unpredictable errors running the code.</b></p>
   */
  public static void compile(String namespace, String outputDir) {
    try(AutoCloseable binder = makeThreadBindings(hashmap(compilePathVar, outputDir))) {
      compileFn.invoke(symbol(namespace));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
