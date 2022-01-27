package tech.v3.datatype;

import clojure.lang.IFn;
import clojure.lang.Keyword;
import tech.v3.datatype.UnaryPredicate;
import static tech.v3.Clj.*;

/**
 * Short form common scalar predicates for use in filtering operations.
 * tech.v3.datatype.UnaryPredicate derives from clojure.lang.IFn,
 * java.util.DoublePredicate, and defines a set of primitive comparison interfaces allowing
 * efficient non-boxing comparison operations.  On the other hand some of the primitives,
 * such as 'eq', are stronger in that they show correct behavior w/r/t Double/NaN.
 *
 * For ordering functions the argument is passed on the lefthand side so Pred.lt(5.0) creates
 * the function `arg &lt; 5.0` where arg is the argument to the function.
 */
public class Pred {
  private Pred(){}
  static final IFn unpredFn = requiringResolve("tech.v3.datatype.binary-pred", "unary-pred");
  static final Keyword eqKwd = keyword("tech.numerics", "eq");
  static final Keyword noteqKwd = keyword("tech.numerics", "not-eq");
  static final Keyword gtKwd = keyword("tech.numerics", ">");
  static final Keyword gteKwd = keyword("tech.numerics", ">=");
  static final Keyword ltKwd = keyword("tech.numerics", "<");
  static final Keyword lteKwd = keyword("tech.numerics", "<=");

  /** `constant == arg`.  Works for Double/NaN. */
  public static UnaryPredicate eq(Object constant) {
    return (UnaryPredicate)unpredFn.invoke(constant, eqKwd);
  }
  /** `constant != arg`.  Works for Double/NaN. */
  public static UnaryPredicate notEq(Object constant) {
    return (UnaryPredicate)unpredFn.invoke(constant, noteqKwd);
  }
  /** `arg &gt; constant`*/
  public static UnaryPredicate gt(Object constant) {
    return (UnaryPredicate)unpredFn.invoke(constant, ltKwd);
  }
  /** `arg &lt; constant`*/
  public static UnaryPredicate lt(Object constant) {
    return (UnaryPredicate)unpredFn.invoke(constant, gtKwd);
  }
  /** `arg &gt= constant`*/
  public static UnaryPredicate gte(Object constant) {
    return (UnaryPredicate)unpredFn.invoke(constant, lteKwd);
  }
  /** `arg &lt= constant`*/
  public static UnaryPredicate lte(Object constant) {
    return (UnaryPredicate)unpredFn.invoke(constant, gteKwd);
  }
  /** `constant == null`*/
  public static UnaryPredicate nil(Object constant) {
    return (UnaryPredicate)unpredFn.invoke(constant, eqKwd);
  }
}
