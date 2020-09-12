package tech.v3.datatype;


import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongComparator;
import it.unimi.dsi.fastutil.doubles.DoubleComparator;


public class Comparators
{
  public interface IntComp extends IntComparator
  {
    default int compare(int lhs, int rhs)
    {
      return compareInts(lhs, rhs);
    }
    public int compareInts(int lhs, int rhs);
  }
  public interface LongComp extends LongComparator
  {
    default int compare(long lhs, long rhs)
    {
      return compareLongs(lhs, rhs);
    }
    public int compareLongs(long lhs, long rhs);
  }
  public interface DoubleComp extends DoubleComparator
  {
    default int compare(double lhs, double rhs)
    {
      return compareDoubles(lhs, rhs);
    }
    public int compareDoubles(double lhs, double rhs);
  }
};
