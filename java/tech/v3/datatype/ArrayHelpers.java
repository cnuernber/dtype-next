package tech.v3.datatype;



public class ArrayHelpers
{
  public static void aset(boolean[] data, int idx, boolean val) {
    data[idx] = val;
  }
  public static void aset(byte[] data, int idx, byte val) {
    data[idx] = val;
  }
  public static void aset(short[] data, int idx, short val) {
    data[idx] = val;
  }
  public static void aset(char[] data, int idx, char val) {
    data[idx] = val;
  }
  public static void aset(int[] data, int idx, int val) {
    data[idx] = val;
  }
  public static void aset(long[] data, int idx, long val) {
    data[idx] = val;
  }
  public static void aset(float[] data, int idx, float val) {
    data[idx] = val;
  }
  public static void aset(double[] data, int idx, double val) {
    data[idx] = val;
  }
  public static void aset(Object[] data, int idx, Object val) {
    data[idx] = val;
  }

  public static void accumPlus(byte[] data, int idx, byte val) {
    data[idx] += val;
  }
  public static void accumPlus(short[] data, int idx, short val) {
    data[idx] += val;
  }
  public static void accumPlus(char[] data, int idx, char val) {
    data[idx] += val;
  }
  public static void accumPlus(int[] data, int idx, int val) {
    data[idx] += val;
  }
  public static void accumPlus(long[] data, int idx, long val) {
    data[idx] += val;
  }
  public static void accumPlus(float[] data, int idx, float val) {
    data[idx] += val;
  }
  public static void accumPlus(double[] data, int idx, double val) {
    data[idx] += val;
  }

  public static void accumMul(byte[] data, int idx, byte val) {
    data[idx] *= val;
  }
  public static void accumMul(short[] data, int idx, short val) {
    data[idx] *= val;
  }
  public static void accumMul(char[] data, int idx, char val) {
    data[idx] *= val;
  }
  public static void accumMul(int[] data, int idx, int val) {
    data[idx] *= val;
  }
  public static void accumMul(long[] data, int idx, long val) {
    data[idx] *= val;
  }
  public static void accumMul(float[] data, int idx, float val) {
    data[idx] *= val;
  }
  public static void accumMul(double[] data, int idx, double val) {
    data[idx] *= val;
  }

  //For some sizes 4 or less manual copy is faster than system.arraycopy.
  //Interestingly enough, this differs depending on of this is an object array
  //or a primitive array with the breakeven for an object array being around 8
  public static void manualCopy(Object[] src, int soff, Object[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }
  public static void manualCopy(byte[] src, int soff, byte[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }
  public static void manualCopy(short[] src, int soff, short[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }
  public static void manualCopy(char[] src, int soff, char[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }
  public static void manualCopy(int[] src, int soff, int[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }
  public static void manualCopy(long[] src, int soff, long[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }
  public static void manualCopy(float[] src, int soff, float[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }
  public static void manualCopy(double[] src, int soff, double[] dst, int doff, int len) {
    int send = soff + len;
    for (; soff < send; ++soff, ++doff)
      dst[doff] = src[soff];
  }

}
