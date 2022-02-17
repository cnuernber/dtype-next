package tech.v3.datatype;


import clojure.lang.IFn;
import clojure.lang.RT;


public class VariableRollingIterBase
{
  long startIdx;
  long targetIdx;
  long endIdx;
  public final Buffer data;
  public final long nElems;
  public final double stepsize;
  public final double window;
  public final IFn compFn;

  public VariableRollingIterBase(Buffer _data, double _stepsize, double _window, IFn _compFn) {
    startIdx = 0;
    endIdx = 0;
    targetIdx = 0;
    data = _data;
    nElems = _data.lsize();
    stepsize = _stepsize;
    window = _window;
    compFn = _compFn;
  }

  public long getStartIdx() { return startIdx; }
  public long getTargetIdx() { return targetIdx; }
  public long getEndIdx() { return endIdx; }
  public long getRangeLength() { return endIdx - startIdx; }
  //End is really just past the end
  public void setEndToTarget() { endIdx = targetIdx + 1; }
  public void setStartToTarget() { startIdx = targetIdx; }
  public void incrementStart() {
    Object curVal = data.readObject(targetIdx);
    while((startIdx < targetIdx) &&
	  ((RT.uncheckedDoubleCast(compFn.invoke(curVal, data.readObject(startIdx)))) >= window)) {
      ++startIdx;
    }
  }
  public void incrementEnd() {
    Object curVal = data.readObject(targetIdx);
    while((endIdx < nElems) &&
	  ((RT.uncheckedDoubleCast(compFn.invoke(data.readObject(endIdx), curVal))) < window)) {
      ++endIdx;
    }
  }
  public void incrementTarget() {
    if (stepsize == 0.0) {
      ++targetIdx;
    } else {
      Object curVal = data.readObject(targetIdx);
      while((targetIdx < nElems) &&
	    ((RT.uncheckedDoubleCast(compFn.invoke(data.readObject(targetIdx), curVal))) < stepsize)) {
	++targetIdx;
      }
    }
  }
  public boolean hasNext() {
    return targetIdx < nElems;
  }
}
