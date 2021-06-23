package tech.v3.datatype;

import clojure.lang.RT;
import java.util.function.BiFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.Arrays;



public final class Convolve1D {
  public static enum Mode
  {
    Full,
    Same,
    Valid,
  };
  public static enum EdgeMode {
    Clamp,
    Nearest, //Synonym for Clamp
    Reflect,
    Constant,
    //Mirror, - This one makes no sense
    Wrap,
    Zero,
  }
  public static class Edging {
    public final EdgeMode mode;
    public final double constant;
    public Edging(EdgeMode mode) {
      this(mode,0.0);
    }
    public Edging(EdgeMode _mode, double _constant) {
      mode = _mode;
      constant = _constant;
    }

    public double[] apply(double[] data, int nWin, Mode mode) {
      final int extraLen;
      final int nWinDec = nWin - 1;
      switch(mode) {
      case Full: extraLen = nWinDec * 2; break;
	//Convolving a even sized window means we have to distribute 1 extra on one
	//side of the input or another.
      case Same: extraLen = (nWin % 2) == 0 ? nWinDec : nWin; break;
      case Valid: extraLen = 0; break;
      default: throw new RuntimeException("Unrecognized mode.");
      }

      return apply(data, data.length + extraLen);
    }

    public double[] apply(double[] data, int new_len) {
      int len = data.length;
      if (len == new_len)
	return data;
      int left_overflow = (new_len - len) / 2;
      int right_overflow = new_len - left_overflow - len;
      int max_overflow = Math.max(left_overflow, right_overflow);
      int end_start = len + left_overflow;
      double[] retval = new double[new_len];
      System.arraycopy(data,0,retval,left_overflow,len);
      switch(mode) {
      case Constant:
      case Zero:
	if(constant != 0.0) {
	  Arrays.fill(retval, 0, left_overflow, constant);
	  Arrays.fill(retval, left_overflow+len, new_len - 1, constant);
	}
	break;
      case Nearest:
      case Clamp:
	double left = data[0];
	double right = data[len-1];
	Arrays.fill(retval, 0, left_overflow, left);
	Arrays.fill(retval, left_overflow+len, new_len - 1, right);
	break;

      case Reflect:
	/* abcddcba|abcd|dcbaabcd */
	for(int idx = 0; idx < max_overflow; ++idx) {
	  boolean even = ((idx / len) % 2) == 0;
	  int ary_pos = idx % len;
	  if (! even) ary_pos = len - ary_pos - 1;

	  if (idx < left_overflow)
	    retval[left_overflow - idx - 1] = data[ary_pos];

	  int write_pos = end_start + idx;
	  //Odd values give off-by-one
	  if (write_pos < new_len)
	    retval[write_pos] = data[len-ary_pos-1];
	}
	break;
      case Wrap:
	/* abcdabcd|abcd|abcdabcd */
	for(int idx = 0; idx < max_overflow; ++idx) {
	  int ary_pos = idx % len;
	  if (idx < left_overflow)
	    retval[left_overflow - idx - 1] = data[len - ary_pos - 1];

	  int write_pos = end_start + idx;
	  //Odd values give off-by-one
	  if (write_pos < new_len)
	    retval[write_pos] = data[ary_pos];
	}
	break;
      }
      return retval;
    }
  }
  //Precondition is that ndata is <= nwindow
  //The difference between convolve and correlate is convolve reverses
  //the window.  Callers can reverse the window before passing it in.
  //The reason this works with double-arrays is because it is an inherently
  //n^2 operation so we want to minimize indexing costs.
  public static double[] correlate
    (BiFunction<Long,BiFunction<Long,Long,Object>,Object> parallelizer,
     double[] data,
     double[] window,
     int stepsize,
     Mode mode,
     Edging edging) {

    final int nData = data.length;
    final int nWin = window.length;
    final int nWinDec = nWin - 1;
    final int nResult;
    switch (mode) {
    case Full: nResult = (nData + nWinDec)/stepsize; break;
    case Same: nResult = nData/stepsize; break;
    case Valid: nResult = (nData - nWin + 1)/stepsize; break;
    default: throw new RuntimeException("Unrecognized mode.");
    }


    double[] conv_data = edging.apply(data, nWin, mode);
    double[] retval = new double[nResult];
    parallelizer.apply( new Long(nResult), new BiFunction<Long,Long,Object> () {
	public Object apply(Long sidx, Long glen) {
	  int start_idx = RT.intCast(sidx);
	  int group_len = RT.intCast(glen);
	  int end_idx = start_idx + group_len;

	  //This loop is an interesting case for a blog post on the vectorization API.
	  for(int idx = start_idx; idx < end_idx; ++idx ) {
	    int data_start_idx = (idx*stepsize);
	    double sum = 0.0;
	    for (int win_idx = 0; win_idx < nWin; ++win_idx) {
	      int data_idx = data_start_idx + win_idx;
	      //System.out.println(String.format("idx %d, win_idx %d, data_idx %d, data_len %d",
	      //			       idx, win_idx, data_idx, conv_len));
	      sum += conv_data[data_idx] * window[win_idx];
	    }
	    retval[idx] = sum;
	  }
	  return null;
	}
      });
    return retval;
  }
}
