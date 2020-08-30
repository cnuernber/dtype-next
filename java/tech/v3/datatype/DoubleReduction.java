package tech.v3.datatype;


public interface DoubleReduction
{
  default double initialize(double value) { return value; }
  double update (double accum, double value);
  default double merge(double lhs, double rhs) { return update(lhs, rhs); }
  default double finalize(double accum, long nElems) { return accum; }
}
