package tech.v3.datatype;


import clojure.lang.Keyword;
import java.util.function.BiPredicate;
import java.util.Comparator;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;
import ham_fisted.IFnDef;


public interface BinaryPredicate extends ElemwiseDatatype, IFnDef.OOO,
					 BiPredicate
{
  default boolean binaryLong(long lhs, long rhs) { return binaryObject(lhs,rhs); }
  default boolean binaryDouble(double lhs, double rhs) { return binaryDouble(lhs,rhs); }
  boolean binaryObject(Object lhs, Object rhs);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object lhs, Object rhs) { return binaryObject(lhs, rhs); }
  default boolean test(Object lhs, Object rhs) {
    return binaryObject(lhs, rhs);
  }
  default LongComparator asLongComparator() {
    return new LongComparator() {
      public int compare(long lhs, long rhs ) {
	if (binaryLong(lhs,rhs)) {
	  return -1;
	} else if (binaryLong(rhs,lhs)) {
	  return 1;
	}
	else
	  return 0;
      }
    };
  }
  default DoubleComparator asDoubleComparator() {
    return new DoubleComparator() {
      public int compare(double lhs, double rhs ) {
	if (binaryDouble(lhs,rhs)) {
	  return -1;
	} else if (binaryDouble(rhs,lhs)) {
	  return 1;
	}
	else
	  return 0;
      }
    };
  }
  default Comparator asComparator() {
    return new Comparator() {
      public int compare(Object lhs, Object rhs ) {
	if (binaryObject(lhs,rhs)) {
	  return -1;
	} else if (binaryObject(rhs,lhs)) {
	  return 1;
	}
	else
	  return 0;
      }
    };
  }
}
