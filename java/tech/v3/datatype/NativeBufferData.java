package tech.v3.datatype;


public class NativeBufferData {
  public final long address;
  public final long nElems;
  public final Object datatype;
  public final Object endianness;
  public final Object metadata;
  public final Object parent;
  public NativeBufferData(long add, long ne, Object dt, Object ed, Object md,
			  Object pt) {
    address = add;
    nElems = ne;
    datatype = dt;
    endianness = ed;
    metadata = md;
    parent = pt;
  }
}
