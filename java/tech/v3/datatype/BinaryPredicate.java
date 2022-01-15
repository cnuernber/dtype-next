package tech.v3.datatype;


import clojure.lang.Keyword;
import clojure.lang.ISeq;
import java.util.function.Function;
import java.util.function.BiPredicate;
import java.util.Comparator;
import it.unimi.dsi.fastutil.bytes.ByteComparator;
import it.unimi.dsi.fastutil.shorts.ShortComparator;
import it.unimi.dsi.fastutil.chars.CharComparator;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.floats.FloatComparator;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;


public interface BinaryPredicate extends ElemwiseDatatype, IFnDef, Function,
					 BiPredicate
{
  boolean binaryBoolean(boolean lhs, boolean rhs);
  boolean binaryByte(byte lhs, byte rhs);
  boolean binaryShort(short lhs, short rhs);
  boolean binaryChar(char lhs, char rhs);
  boolean binaryInt(int lhs, int rhs);
  boolean binaryLong(long lhs, long rhs);
  boolean binaryFloat(float lhs, float rhs);
  boolean binaryDouble(double lhs, double rhs);
  boolean binaryObject(Object lhs, Object rhs);
  default Object elemwiseDatatype () { return Keyword.intern(null, "object"); }
  default Object invoke(Object lhs, Object rhs) { return binaryObject(lhs, rhs); }
  default Object applyTo(ISeq seq) {
    if (2 != seq.count()) {
      throw new RuntimeException("Argument count incorrect for binary op");
    }
    return invoke(seq.first(), seq.next().first());
  }
  default boolean test(Object lhs, Object rhs) {
    return binaryObject(lhs, rhs);
  }
  default ByteComparator asByteComparator() {
    return new ByteComparator() {
      public int compare(byte lhs, byte rhs ) {
	if (binaryByte(lhs,rhs)) {
	  return -1;
	} else if (binaryByte(rhs,lhs)) {
	  return 1;
	}
	else
	  return 0;	 
      }
    };
  }
  default ShortComparator asShortComparator() {
    return new ShortComparator() {
      public int compare(short lhs, short rhs ) {
	if (binaryShort(lhs,rhs)) {
	  return -1;
	} else if (binaryShort(rhs,lhs)) {
	  return 1;
	}
	else
	  return 0;	 
      }
    };
  }
  default CharComparator asCharComparator() {
    return new CharComparator() {
      public int compare(char lhs, char rhs ) {
	if (binaryChar(lhs,rhs)) {
	  return -1;
	} else if (binaryChar(rhs,lhs)) {
	  return 1;
	}
	else
	  return 0;	 
      }
    };
  }
  default IntComparator asIntComparator() {
    return new IntComparator() {
      public int compare(int lhs, int rhs ) {
	if (binaryInt(lhs,rhs)) {
	  return -1;
	} else if (binaryInt(rhs,lhs)) {
	  return 1;
	}
	else
	  return 0;	 
      }
    };
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
  default FloatComparator asFloatComparator() {
    return new FloatComparator() {
      public int compare(float lhs, float rhs ) {
	if (binaryFloat(lhs,rhs)) {
	  return -1;
	} else if (binaryFloat(rhs,lhs)) {
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
