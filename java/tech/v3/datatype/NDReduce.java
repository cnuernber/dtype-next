package tech.v3.datatype;


import clojure.lang.IFn;
import clojure.lang.RT;


public class NDReduce {
  public static Object ndReduce1D(NDBuffer buf, Object rfn, Object acc,
				  long sidx, long eidx) {
    IFn.LOO reducer;
    if(rfn instanceof IFn.OLO) {
      final IFn.OLO rf = (IFn.OLO)rfn;
      reducer = new IFn.LOO() {
	  public Object invokePrim(long idx, Object acc) {
	    return rf.invokePrim(acc, buf.ndReadLong(idx));
	  }};
    } else if (rfn instanceof IFn.ODO) {
      final IFn.ODO rf = (IFn.ODO)rfn;
      reducer = new IFn.LOO() {
	  public Object invokePrim(long idx, Object acc) {
	    return rf.invokePrim(acc, buf.ndReadDouble(idx));
	  }};
    } else {
      final IFn rf = (IFn)rfn;
      reducer = new IFn.LOO() {
	  public Object invokePrim(long idx, Object acc) {
	    return rf.invoke(acc, buf.ndReadObject(idx));
	  }};
    }
    for(long s = sidx; s < eidx && !RT.isReduced(acc); ++s) {
      acc = reducer.invokePrim(s,acc);
    }
    return acc;
  }
  public static long round(long v, long base) {
    return v - (v % base);
  }
  public static Object ndReduce2D(NDBuffer buf, long nc, Object rfn, Object acc,
				  long sidx, long eidx) {
    IFn.LLOO reducer;
    if(rfn instanceof IFn.OLO) {
      final IFn.OLO rf = (IFn.OLO)rfn;
      reducer = new IFn.LLOO() {
	  public Object invokePrim(long x, long c, Object acc) {
	    return rf.invokePrim(acc, buf.ndReadLong(x,c));
	  }};
    } else if (rfn instanceof IFn.ODO) {
      final IFn.ODO rf = (IFn.ODO)rfn;
      reducer = new IFn.LLOO() {
	  public Object invokePrim(long x, long c, Object acc) {
	    return rf.invokePrim(acc, buf.ndReadDouble(x,c));
	  }};
    } else {
      final IFn rf = (IFn)rfn;
      reducer = new IFn.LLOO() {
	  public Object invokePrim(long x, long c, Object acc) {
	    return rf.invoke(acc, buf.ndReadObject(x,c));
	  }};
    }
    for(long xidx = sidx; xidx < eidx && !RT.isReduced(acc);) {
	final long nextX = Math.min(eidx, round(xidx+nc, nc));
	final long xcoord = xidx / nc;
	final long startC = xidx % nc;
	final long endC = startC + nextX - xidx;
	for(long cidx = startC; cidx < endC && !RT.isReduced(acc); ++cidx)
	  acc = reducer.invokePrim(xcoord, cidx, acc);
	xidx = nextX;
    }
    return acc;
  }

  public static Object ndReduce3D(NDBuffer buf, long nx, long nc, Object rfn, Object acc,
				  long sidx, long eidx) {
    IFn.LLLOO reducer;
    if(rfn instanceof IFn.OLO) {
      final IFn.OLO rf = (IFn.OLO)rfn;
      reducer = new IFn.LLLOO() {
	  public Object invokePrim(long y, long x, long c, Object acc) {
	    return rf.invokePrim(acc, buf.ndReadLong(y,x,c));
	  }};
    } else if (rfn instanceof IFn.ODO) {
      final IFn.ODO rf = (IFn.ODO)rfn;
      reducer = new IFn.LLLOO() {
	  public Object invokePrim(long y, long x, long c, Object acc) {
	    return rf.invokePrim(acc, buf.ndReadDouble(y,x,c));
	  }};
    } else {
      final IFn rf = (IFn)rfn;
      reducer = new IFn.LLLOO() {
	  public Object invokePrim(long y, long x, long c, Object acc) {
	    return rf.invoke(acc, buf.ndReadObject(y,x,c));
	  }};
    }
    final long ys = nx * nc;
    for(long yidx = sidx; yidx < eidx && !RT.isReduced(acc);) {
      final long nextY = Math.min(eidx, round(yidx+ys,ys));
      final long ycoord = yidx / ys;
      for(long xidx = yidx; xidx < nextY && !RT.isReduced(acc);) {
	final long nextX = Math.min(nextY, round(xidx+nc, nc));
	final long xcoord = (xidx % ys) / nc;
	final long startC = xidx % nc;
	final long numC = nextX - xidx;
	final long endC = startC + numC;
	for(long cidx = startC; cidx < endC && !RT.isReduced(acc); ++cidx)
	  acc = reducer.invokePrim(ycoord, xcoord, cidx, acc);
	xidx = nextX;
      }
      yidx = nextY;
    }
    return acc;
  }

}
