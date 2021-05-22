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
  //Precondition is that ndata is <= nwindow
  public static double[] convolve
    (BiFunction<Long,BiFunction<Long,Long,Object>,Object> parallelizer,
     Buffer data,
     Buffer rev_window,
     DoubleUnaryOperator in_finalize,
     Mode mode) {

    final int nData = data.size();
    final int nWin = rev_window.size();
    if (nData < nWin)
      throw new RuntimeException("Data size must be <= window size");
    final int nWinDec = nWin - 1;
    final int nResult;
    switch (mode) {
    case Full: nResult = nData + nWinDec; break;
    case Same: nResult = nData; break;
    case Valid: nResult = nData - nWin + 1; break;
    default: throw new RuntimeException("Unrecognized mode.");
    }

    if (in_finalize == null) {
      in_finalize = new DoubleUnaryOperator () {
	  public double applyAsDouble(double val) { return val; }
	};
    }
    final DoubleUnaryOperator finalize = in_finalize;
    double[] window = new double[nWin];
    for (int idx = 0; idx < nWin; ++idx) {
      window[idx] = rev_window.readDouble(nWinDec - idx);
    }
    double[] retval = new double[nResult];
    parallelizer.apply( new Long(nResult), new BiFunction<Long,Long,Object> () {
	public Object apply(Long sidx, Long glen) {
	  int start_idx = RT.intCast(sidx);
	  int group_len = RT.intCast(glen);
	  int end_idx = start_idx + group_len;
	  final int windowOffset;
	  switch(mode) {
	  case Full: windowOffset = nWinDec; break;
	  case Same: windowOffset = (nWinDec / 2); break;
	  case Valid: windowOffset = 0; break;
	  default: throw new RuntimeException("Unrecognized mode.");
	  }
	  for(int idx = start_idx; idx < end_idx; ++idx ) {
	    int data_start_idx = idx - windowOffset;
	    double sum = 0.0;
	    for (int win_idx = 0; win_idx < nWin; ++win_idx) {
	      int data_idx = data_start_idx + win_idx;
	      double data_val = 0.0;
	      if (data_idx < nData && data_idx >= 0)
		data_val = data.readDouble(data_idx);
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
