package tech.v3.datatype;


public final class Complex
{
  public static double mulReal( double ar, double ai, double br, double bi) {
    return ar*br - ai*bi;
  }
  public static double mulImg( double ar, double ai, double br, double bi) {
    return ar*bi + ai*br;
  }
  public static double[] mul(double[] lhs, int lhsOffset, double[] rhs, int rhsOffset,
			     double[] result, int resOffset, int nElems) {
    final int lhsOff = lhsOffset * 2;
    final int rhsOff = rhsOffset * 2;
    final int resOff = resOffset * 2;
    for (int idx = 0; idx < nElems; ++idx ) {
      int localIdx = idx*2;
      result[localIdx+resOff] = mulReal(lhs[localIdx + lhsOff],
					lhs[localIdx + 1 + lhsOff],
					rhs[localIdx + rhsOff],
					rhs[localIdx + 1 + rhsOff]);
      result[localIdx+1+resOff] = mulImg(lhs[localIdx + lhsOff],
					 lhs[localIdx + 1 + lhsOff],
					 rhs[localIdx + rhsOff],
					 rhs[localIdx + 1 + rhsOff]);
    }
    return result;
  }
  public static double[] mul(double[] lhs, double[] rhs) {
    return mul(lhs, 0, rhs, 0, new double[lhs.length], 0,
	       lhs.length/2);
  }
  public static double[] realToComplex(double[] real, int off, double[] complex,
				       int coff, int nElems) {
    final int coffset = coff * 2;
    for( int idx = 0; idx < nElems; ++idx ) {
      final int localCOff = coffset + idx*2;
      complex[localCOff] = real[idx+off];
      complex[localCOff+1] = 0.0;
    }
    return complex;
  }
  public static double[] realToComplex(double[] real) {
    return realToComplex(real, 0, new double[real.length*2], 0, real.length);
  }
  public static double[] realToComplex(double real, double[] complex, int coff,
				       int nElems) {
    final int coffset = coff * 2;
    for( int idx = 0; idx < nElems; ++idx ) {
      final int localCOff = coffset + idx*2;
      complex[localCOff] = real;
      complex[localCOff+1] = 0.0;
    }
    return complex;
  }
  public static double[] complexToReal(double[] complex, int coff, double[] real,
				       int off, int nElems) {
    final int coffset = coff * 2;
    for(int idx = 0; idx < nElems; ++idx ) {
      real[idx+off] = complex[coffset + idx*2];
    }
    return real;
  }
  public static double[] complexToReal(double[] complex) {
    return complexToReal(complex, 0, new double[complex.length/2], 0, complex.length/2);
  }
}
