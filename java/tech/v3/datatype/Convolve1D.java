package tech.v3.datatype;

import clojure.lang.RT;
import java.util.function.BiFunction;
import java.util.function.DoubleUnaryOperator;



public final class Convolve1D {
  public static enum Mode
  {
    Full,
    Same,
    Valid,
  };
  public static enum EdgeMode {
    Clamp,
    Zero,
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
     DoubleUnaryOperator in_finalize,
     Mode mode,
     EdgeMode edgeMode) {

    final int nData = data.length;
    final int nWin = window.length;
    if (nData < nWin)
      throw new RuntimeException("Data size must be <= window size");
    final int nWinDec = nWin - 1;
    final int nResult;
    switch (mode) {
    case Full: nResult = (nData + nWinDec)/stepsize; break;
    case Same: nResult = nData/stepsize; break;
    case Valid: nResult = (nData - nWin + 1)/stepsize; break;
    default: throw new RuntimeException("Unrecognized mode.");
    }

    if (in_finalize == null) {
      in_finalize = new DoubleUnaryOperator () {
	  public double applyAsDouble(double val) { return val; }
	};
    }
    final DoubleUnaryOperator finalize = in_finalize;


    final int windowOffset;
    switch(mode) {
    case Full: windowOffset = nWinDec; break;
    case Same: windowOffset = (nWinDec / 2); break;
    case Valid: windowOffset = 0; break;
    default: throw new RuntimeException("Unrecognized mode.");
    }


    final double left_edge;
    final double right_edge;
    switch(edgeMode) {
    case Clamp: left_edge=data[0]; right_edge=data[nData-1]; break;
    case Zero: left_edge=0.0; right_edge=0.0; break;
    default: throw new RuntimeException("Unrecognized edge mode");
    }


    double[] retval = new double[nResult];
    parallelizer.apply( new Long(nResult), new BiFunction<Long,Long,Object> () {
	public Object apply(Long sidx, Long glen) {
	  int start_idx = RT.intCast(sidx);
	  int group_len = RT.intCast(glen);
	  int end_idx = start_idx + group_len;

	  //This loop is an interesting case for a blog post on the vectorization API.
	  for(int idx = start_idx; idx < end_idx; ++idx ) {
	    int data_start_idx = (idx*stepsize) - windowOffset;
	    double sum = 0.0;
	    for (int win_idx = 0; win_idx < nWin; ++win_idx) {
	      int data_idx = data_start_idx + win_idx;
	      double data_val;
	      if (data_idx < 0)
		data_val = left_edge;
	      else if (data_idx >= nData)
		data_val = right_edge;
	      else
		data_val = data[data_idx];
	      sum += data_val * window[win_idx];
	    }
	    retval[idx] = finalize.applyAsDouble(sum);
	  }
	  return null;
	}
      });
    return retval;
  }
}
