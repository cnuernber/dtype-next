package tech.v3.datatype;


public class ArrayBufferData {
  public final Object arrayData;
  public final long offset;
  public final long nElems;
  public final Object datatype;
  public final Object metadata;
  public ArrayBufferData(Object _data, long off, long ne, Object dt, Object md) {
    arrayData = _data;
    offset = off;
    nElems = ne;
    datatype = dt;
    metadata = md;
  }
}
