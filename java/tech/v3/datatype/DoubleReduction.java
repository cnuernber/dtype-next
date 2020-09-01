package tech.v3.datatype;


public interface DoubleReduction
{
  default double elemwise(double value) { return value; }
  default double update (double accum, double value) { return accum + value; }
  default double merge (double accum, double value) { return accum + value; }
  default double finalize(double accum, long nElems) { return accum; }
}
