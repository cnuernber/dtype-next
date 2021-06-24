package tech.v3.datatype;


import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorShuffle;


public final class VecOps
{

  public static double sum(double[] data, int off, int len) {
    VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
    int vecLen = species.length();
    int dlen = len;

    int nVecGroups = dlen / vecLen;
    int nVecGroupsDec = nVecGroups-1;
    int leftover = dlen % vecLen;
    DoubleVector vsum = DoubleVector.broadcast(species, 0.0);
    for(int idx = 0; idx < nVecGroups; ++idx) {
      //Attempt prefetch - made no difference or slightly worse
      //if (idx < nVecGroupsDec)
      //DoubleVector.fromArray(species, data, (idx + 1) * vecLen + off);
      vsum = vsum.add(DoubleVector.fromArray(species, data, idx * vecLen + off));
    }
    double sum = vsum.reduceLanes(VectorOperators.ADD);
    int endOffset = nVecGroups * vecLen + off;
    for(int idx = 0; idx < leftover; ++idx ) {
      sum = sum + data[idx+endOffset];
    }
    return sum;
  }
  
  public static double[] minmaxsum(double[] data, int off, int len) {
    VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
    int vecLen = species.length();
    int dlen = len;

    int nVecGroups = dlen / vecLen;
    int nVecGroupsDec = nVecGroups-1;
    int leftover = dlen % vecLen;
    DoubleVector vsum = DoubleVector.broadcast(species, 0.0);
    DoubleVector vmin = DoubleVector.broadcast(species,Double.MAX_VALUE);
    DoubleVector vmax = DoubleVector.broadcast(species,Double.MIN_VALUE);
    for(int idx = 0; idx < nVecGroups; ++idx) {
      DoubleVector dval = DoubleVector.fromArray(species, data, (idx * vecLen) + off);
      vsum = vsum.add(dval);
      vmin = vmin.min(dval);
      vmax = vmax.max(dval);
    }
    double sum = vsum.reduceLanes(VectorOperators.ADD);
    double minval = vmin.reduceLanes(VectorOperators.MIN);
    double maxval = vmax.reduceLanes(VectorOperators.MAX);
    int endOffset = nVecGroups * vecLen + off;
    for(int idx = 0; idx < leftover; ++idx ) {
      double dval = data[idx+endOffset];
      sum = sum + dval;
      minval = Math.min(minval, dval);
      maxval = Math.max(maxval, dval);
    }
    return new double[]{minval,maxval,sum};
  }
  //Not able to make this faster than naive loop.
  public static double[] cumsum(double[] data, int off, int len) {
    //We have to fix the species
    VectorSpecies<Double> species = DoubleVector.SPECIES_256;
    int vecLen = 4;
    int dlen = len;
    int dVecLen = dlen / vecLen;
    int leftover = dlen % vecLen;
    double[] result = new double[len];
    double sum = 0.0;
    VectorShuffle<Double> shuf1 = VectorShuffle.fromValues(species, 0, 0, 1, 2);
    VectorMask<Double> m1 = VectorMask.fromValues(species, false, true, true, true);
    VectorShuffle<Double> shuf2 = VectorShuffle.fromValues(species, 0, 0, 0, 1);
    VectorMask<Double> m2 = VectorMask.fromValues(species, false, false, true, true);
    VectorShuffle<Double> shuf3 = VectorShuffle.fromValues(species, 0, 0, 0, 0);
    VectorMask<Double> m3 = VectorMask.fromValues(species, false, false, false, true);
    for (int idx = 0; idx < dVecLen; ++idx ) {
      int vecidx = idx * vecLen;
      //carry summation from previous iteration
      DoubleVector input = DoubleVector.fromArray(species, data, off + vecidx);
      DoubleVector finalVec = input.add(input.rearrange(shuf1), m1)
	.add(input.rearrange(shuf2),m2)
	.add(input.rearrange(shuf3),m3)
	.add(sum);
      sum = finalVec.lane(3);
      finalVec.intoArray(result,vecidx);
    }
    int finalOff = dVecLen * vecLen;
    for (int idx = 0; idx < leftover; ++idx) {
      sum += data[finalOff + idx];
      result[finalOff+idx] = sum;
    }
    return result;
  }
  public static double dot(double[] d1, int of1, double[] d2, int of2, int len) {
    VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
    int vecLen = species.length();
    int nVec = len / vecLen;
    int leftover = len % vecLen;
    DoubleVector vsum = DoubleVector.broadcast(species, 0.0);
    for (int idx = 0; idx < nVec; ++idx ) {
      int vecoff = idx * vecLen;
      DoubleVector lhs = DoubleVector.fromArray(species, d1, of1 + vecoff);
      DoubleVector rhs = DoubleVector.fromArray(species, d2, of2 + vecoff);
      vsum = lhs.fma(rhs,vsum);
    }
    double sum = vsum.reduceLanes(VectorOperators.ADD);
    int voff = nVec * vecLen;
    for (int idx = 0; idx < leftover; ++idx ) {
      sum += d1[idx + of1 + voff] * d2[idx + of2 + voff];
    }
    return sum;
  }

  public static double magnitudeSquared(double[] data, int off, int len) {
    VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
    int vecLen = species.length();
    int dlen = len;

    int nVecGroups = dlen / vecLen;
    int nVecGroupsDec = nVecGroups-1;
    int leftover = dlen % vecLen;
    DoubleVector vsum = DoubleVector.broadcast(species, 0.0);
    for(int idx = 0; idx < nVecGroups; ++idx) {
      //Attempt prefetch - made no difference or slightly worse
      //if (idx < nVecGroupsDec)
      //DoubleVector.fromArray(species, data, (idx + 1) * vecLen + off);
      DoubleVector entry = DoubleVector.fromArray(species, data, idx * vecLen + off);
      vsum = entry.fma(entry,vsum);
    }
    double sum = vsum.reduceLanes(VectorOperators.ADD);
    int endOffset = nVecGroups * vecLen + off;
    for(int idx = 0; idx < leftover; ++idx ) {
      sum = sum + data[idx+endOffset];
    }
    return sum;
  }

  public static double distanceSquared(double[] d1, int of1, double[] d2, int of2, int len) {
    VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
    int vecLen = species.length();
    int nVec = len / vecLen;
    int leftover = len % vecLen;
    DoubleVector vsum = DoubleVector.broadcast(species, 0.0);
    for (int idx = 0; idx < nVec; ++idx ) {
      int vecoff = idx * vecLen;
      DoubleVector lhs = DoubleVector.fromArray(species, d1, of1 + vecoff);
      DoubleVector rhs = DoubleVector.fromArray(species, d2, of2 + vecoff);
      DoubleVector diff = lhs.sub(rhs);
      vsum = diff.fma(diff,vsum);
    }
    double sum = vsum.reduceLanes(VectorOperators.ADD);
    int voff = nVec * vecLen;
    for (int idx = 0; idx < leftover; ++idx ) {
      double temp = d1[idx + of1 + voff] - d2[idx + of2 + voff];
      sum += temp * temp;
    }
    return sum;
  }

  public static double[] submul(double[] d1, int off, int len,
				double subval, double mulval,
				double[] result, int resoff) {
    VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
    int vecLen = species.length();
    int nVec = len / vecLen;
    int leftover = len % vecLen;
    for (int idx = 0; idx < nVec; ++idx) {
      int vecoff = idx * vecLen;
      DoubleVector.fromArray(species, d1, off + vecoff)
	.sub(subval)
	.mul(mulval)
	.intoArray(result, vecoff + resoff);
    }
    int voff = vecLen * nVec + resoff;
    for (int idx=0; idx < leftover; ++idx) {
      result[voff + idx] = (d1[voff + idx] - subval) * mulval;
    }
    return result;
  }

  public static double[] muladd(double[] d1, int off, int len,
				double mulval, double addval) {
    VectorSpecies<Double> species = DoubleVector.SPECIES_PREFERRED;
    double[] retval = new double[len];
    int vecLen = species.length();
    int nVec = len / vecLen;
    int leftover = len % vecLen;
    for (int idx = 0; idx < nVec; ++idx) {
      int vecoff = idx * vecLen;
      DoubleVector.fromArray(species, d1, off + vecoff)
	.fma(mulval, addval)
	.intoArray(retval, vecoff);
    }
    int voff = vecLen * nVec;
    for (int idx=0; idx < leftover; ++idx) {
      retval[voff + idx] = (d1[voff + idx] * mulval) + addval;
    }
    return retval;
  }
}
